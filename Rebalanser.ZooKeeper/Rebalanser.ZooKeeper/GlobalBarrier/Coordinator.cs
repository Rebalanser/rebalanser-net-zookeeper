using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.ResourceManagement;
using Rebalanser.ZooKeeper.Zk;

namespace Rebalanser.ZooKeeper.GlobalBarrier
{
    public class Coordinator : Watcher, ICoordinator
    {
        // services
        private IZooKeeperService zooKeeperService;
        private ILogger logger;
        private ResourceManager store;
        
        // immutable state
        private CancellationToken coordinatorToken;
        private readonly TimeSpan minimumRebalancingInterval;
        private string clientId;
        
        // mutable state
        private StatusZnode status;
        private Task rebalancingTask;
        private CancellationTokenSource rebalancingCts;
        private int resourcesVersion;
        private BlockingCollection<CoordinatorEvent> events;
        private bool ignoreWatches;
        private RebalancingResult? lastRebalancingResult;

        public Coordinator(IZooKeeperService zooKeeperService,
            ILogger logger,
            ResourceManager store,
            string clientId,
            TimeSpan minimumRebalancingInterval,
            CancellationToken coordinatorToken)
        {
            this.zooKeeperService = zooKeeperService;
            this.logger = logger;
            this.store = store;
            this.minimumRebalancingInterval = minimumRebalancingInterval;
            this.clientId = clientId;
            this.coordinatorToken = coordinatorToken;
            this.rebalancingCts = new CancellationTokenSource();
            this.events = new BlockingCollection<CoordinatorEvent>();
        }
        
        public override async Task process(WatchedEvent @event)
        {
            if (this.coordinatorToken.IsCancellationRequested || this.ignoreWatches)
                return;
            
            if(@event.getPath() != null)
                this.logger.Info(this.clientId, $"Coordinator - KEEPER EVENT {@event.getState()} - {@event.get_Type()} - {@event.getPath()}");
            else 
                this.logger.Info(this.clientId, $"Coordinator - KEEPER EVENT {@event.getState()} - {@event.get_Type()}");
            
            switch (@event.getState())
            {
                case Event.KeeperState.Expired:
                    this.events.Add(CoordinatorEvent.SessionExpired);
                    break;
                case Event.KeeperState.Disconnected:
                    break;
                case Event.KeeperState.ConnectedReadOnly:
                case Event.KeeperState.SyncConnected:
                    if (@event.getPath() != null)
                    {
                        if(@event.get_Type() == Event.EventType.NodeDataChanged)
                        {
                            if (@event.getPath().EndsWith("epoch"))
                                this.events.Add(CoordinatorEvent.NoLongerCoordinator);
                        }
                        else if (@event.get_Type() == Event.EventType.NodeChildrenChanged)
                        {
                            if (@event.getPath().EndsWith("resources"))
                                this.events.Add(CoordinatorEvent.RebalancingTriggered);
                            else if (@event.getPath().EndsWith("clients"))
                                this.events.Add(CoordinatorEvent.RebalancingTriggered);
                        }
                    }

                    break;
                default:
                    this.logger.Error(this.clientId,
                        $"Coordinator - Currently this library does not support ZooKeeper state {@event.getState()}");
                    this.events.Add(CoordinatorEvent.PotentialInconsistentState);
                    break;
            }

            await Task.Yield();
        }

        public async Task<BecomeCoordinatorResult> BecomeCoordinatorAsync(int currentEpoch)
        {
            try
            {
                this.ignoreWatches = false;
                await this.zooKeeperService.IncrementAndWatchEpochAsync(currentEpoch, this);
                await this.zooKeeperService.WatchNodesAsync(this);
                
                var getResourcesRes = await this.zooKeeperService.GetResourcesAsync(this, null);
                this.resourcesVersion = getResourcesRes.Version;
                
                this.status = await this.zooKeeperService.GetStatusAsync();
            }
            catch (ZkStaleVersionException e)
            {
                this.logger.Error(this.clientId, "Could not become coordinator as a stale version number was used", e);
                return BecomeCoordinatorResult.StaleEpoch;
            }
            catch (ZkInvalidOperationException e)
            {
                this.logger.Error(this.clientId, "Could not become coordinator as an invalid ZooKeeper operation occurred", e);
                return BecomeCoordinatorResult.Error;
            }
            
            this.events.Add(CoordinatorEvent.RebalancingTriggered);
            return BecomeCoordinatorResult.Ok;
        }

        public async Task<CoordinatorExitReason> StartEventLoopAsync()
        {
            var rebalanceTimer = new Stopwatch();
            
            while (!this.coordinatorToken.IsCancellationRequested)
            {
                CoordinatorEvent coordinatorEvent;
                if (this.events.TryTake(out coordinatorEvent))
                {
                    switch (coordinatorEvent)
                    {
                        case CoordinatorEvent.SessionExpired:
                            await CleanUpAsync();
                            return CoordinatorExitReason.SessionExpired;
                        
                        case CoordinatorEvent.NoLongerCoordinator:
                            await CleanUpAsync();
                            return CoordinatorExitReason.NoLongerCoordinator;
                        
                        case CoordinatorEvent.PotentialInconsistentState:
                            await CleanUpAsync();
                            return CoordinatorExitReason.PotentialInconsistentState;
                        
                        case CoordinatorEvent.FatalError:
                            await CleanUpAsync();
                            return CoordinatorExitReason.FatalError;
                        
                        case CoordinatorEvent.RebalancingTriggered:
                            if (this.events.Any())
                            {
                                // skip this event. All other events take precedence over rebalancing
                                // there may be multiple rebalancing events, so if the events collection
                                // consists only of rebalancing events then we'll just process the last one
                            }
                            else if (!rebalanceTimer.IsRunning || rebalanceTimer.Elapsed > this.minimumRebalancingInterval)
                            {
                                await CancelRebalancingIfInProgressAsync();
                                rebalanceTimer.Reset();
                                rebalanceTimer.Start();
                                logger.Info(this.clientId, "Coordinator - Rebalancing triggered");
                                rebalancingTask = Task.Run(async () => await TriggerRebalancing(this.rebalancingCts.Token));
                            }
                            else
                            {
                                // if enough time has not passed since the last rebalancing just readd it
                                this.events.Add(CoordinatorEvent.RebalancingTriggered);   
                            }
                            break;
                        default:
                            await CleanUpAsync();
                            return CoordinatorExitReason.PotentialInconsistentState;
                    }
                }

                await WaitFor(1000);
            }

            if (this.coordinatorToken.IsCancellationRequested)
            {
                await CancelRebalancingIfInProgressAsync();
                await this.zooKeeperService.CloseSessionAsync();
                return CoordinatorExitReason.Cancelled;
            }

            return CoordinatorExitReason.PotentialInconsistentState; // if this happens then we have a correctness bug
        }
        
        private async Task CleanUpAsync()
        {
            try
            {
                this.ignoreWatches = true;
                await CancelRebalancingIfInProgressAsync();
            }
            finally
            {
                await this.store.InvokeOnStopActionsAsync(this.clientId, "Coordinator");
            }
        }

        private async Task CancelRebalancingIfInProgressAsync()
        {
            if (this.rebalancingTask != null && !this.rebalancingTask.IsCompleted)
            {
                logger.Info(this.clientId, "Coordinator - Cancelling the rebalancing that is in progress");
                this.rebalancingCts.Cancel();
                try
                {
                    await this.rebalancingTask; // might need to put a time limit on this
                }
                catch (Exception ex)
                {
                    this.logger.Error(this.clientId, "Coordinator - Errored on cancelling rebalancing", ex);
                    this.events.Add(CoordinatorEvent.PotentialInconsistentState);
                }
                this.rebalancingCts = new CancellationTokenSource(); // reset cts
            }
        }

        private async Task WaitFor(int milliseconds)
        {
            try
            {
                await Task.Delay(milliseconds, this.coordinatorToken);
            }
            catch (TaskCanceledException)
            {}
        }
        
        private async Task TriggerRebalancing(CancellationToken rebalancingToken)
        {
            try
            {
                await this.zooKeeperService.WatchResourcesChildrenAsync(this);
                await this.zooKeeperService.WatchNodesAsync(this);

                var result = await RebalanceAsync(rebalancingToken);
                switch (result)
                {
                    case RebalancingResult.Complete:
                        logger.Info(this.clientId, "Coordinator - Rebalancing complete");
                        break;
                    case RebalancingResult.Cancelled:
                        logger.Info(this.clientId, "Coordinator - Rebalancing cancelled");
                        break;
                    case RebalancingResult.NotRequired:
                        logger.Info(this.clientId, "Coordinator - Rebalancing not required");
                        break;
                }
                
                lastRebalancingResult = result;
            }
            catch (ZkSessionExpiredException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                this.logger.Error(this.clientId, "Coordinator - The current session has expired", e);
                this.events.Add(CoordinatorEvent.SessionExpired);
            }
            catch (ZkStaleVersionException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                this.logger.Error(this.clientId,
                    "Coordinator - A stale znode version was used, aborting rebalancing.", e);
                this.events.Add(CoordinatorEvent.NoLongerCoordinator);
            }
            catch (ZkInvalidOperationException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                this.logger.Error(this.clientId,
                    "Coordinator - An invalid ZooKeeper operation occurred, aborting rebalancing.",
                    e);
                if(await this.store.SafeInvokeOnErrorActionsAsync(this.clientId, "Client error", e))
                    this.events.Add(CoordinatorEvent.PotentialInconsistentState);
                else
                    this.events.Add(CoordinatorEvent.FatalError);
            }
            catch (InconsistentStateException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                this.logger.Error(this.clientId,
                    "Coordinator - An error occurred potentially leaving the client in an inconsistent state, aborting rebalancing.",
                    e);
                if(await this.store.SafeInvokeOnErrorActionsAsync(this.clientId, "Client error", e))
                    this.events.Add(CoordinatorEvent.PotentialInconsistentState);
                else
                    this.events.Add(CoordinatorEvent.FatalError);
            }
            catch (TerminateClientException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                this.logger.Error(this.clientId,
                    "Coordinator - A fatal error has occurred, aborting rebalancing.",
                    e);
                await this.store.SafeInvokeOnErrorActionsAsync(this.clientId, "Client error", e);
                this.events.Add(CoordinatorEvent.FatalError);
            }
            catch (ZkOperationCancelledException)
            {
                lastRebalancingResult = RebalancingResult.Cancelled;
                logger.Info(this.clientId, "Coordinator - Rebalancing cancelled");
            }
            catch (Exception e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                this.logger.Error(this.clientId,
                    "Coordinator - An unexpected error has occurred, aborting rebalancing.", e);
                if(await this.store.SafeInvokeOnErrorActionsAsync(this.clientId, "Client error", e))
                    this.events.Add(CoordinatorEvent.PotentialInconsistentState);
                else
                    this.events.Add(CoordinatorEvent.FatalError);
            }
        }

        private async Task<RebalancingResult> RebalanceAsync(CancellationToken rebalancingToken)
        {
            var stopPhaseResult = await StopActivityPhaseAsync(rebalancingToken);
            if (stopPhaseResult.PhaseResult != RebalancingResult.Complete)
                return stopPhaseResult.PhaseResult;
            
            var assignPhaseResult = await AssignResourcesPhaseAsync(rebalancingToken, 
                stopPhaseResult.ResourcesZnode,
                stopPhaseResult.ClientsZnode);
            if (assignPhaseResult.PhaseResult != RebalancingResult.Complete)
                return assignPhaseResult.PhaseResult;

            var verifyPhaseResult = await VerifyStartedPhaseAsync(rebalancingToken);
            if (verifyPhaseResult.PhaseResult != RebalancingResult.Complete)
                return assignPhaseResult.PhaseResult;
            
            return RebalancingResult.Complete;
        }

        private string GetClientId(string clientPath)
        {
            return clientPath.Substring(clientPath.LastIndexOf("/", StringComparison.Ordinal)+1);
        }

        private async Task<RebalancingPhaseResult> StopActivityPhaseAsync(CancellationToken rebalancingToken)
        {
            this.logger.Info(this.clientId, "Coordinator - Get active clients and resources");
            var clients = await this.zooKeeperService.GetActiveClientsAsync();
            var latestResources = await this.zooKeeperService.GetResourcesAsync(null, null);
            
            if (latestResources.Version != this.resourcesVersion)
                throw new ZkStaleVersionException("Resources znode version does not match expected value, indicates another client has been made coordinator and is executing a rebalancing.");
            
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);
            
            // if no resources were changed and there are more clients than resources then check
            // to see if rebalancing is necessary. If existing assignments are still valid then
            // a new client or the loss of a client with no assignments need not trigger a rebalancing
            if (!IsRebalancingRequired(clients, latestResources))
            {
                this.logger.Info(this.clientId, "Coordinator - No rebalancing required. No resource change. No change to existing assigned clients. More clients than resources.");
                return new RebalancingPhaseResult(RebalancingResult.NotRequired);
            }
            
            logger.Info(this.clientId, "Coordinator - Command followers to stop");
            status.RebalancingStatus = RebalancingStatus.StopActivity;
            status.Version = await this.zooKeeperService.SetStatus(status);
            
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);

            await this.store.InvokeOnStopActionsAsync(this.clientId, "Coordinator");
            
            // wait for confirmation that all followers have stopped or for time limit
            while (!rebalancingToken.IsCancellationRequested)
            {
                var stopped = await this.zooKeeperService.GetStoppedAsync();

                if (IsClientListMatch(clients.ClientPaths, stopped))
                    break;
                else
                    await WaitFor(1000); // try again in 1s
            }
            
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);

            var phaseResult = new RebalancingPhaseResult(RebalancingResult.Complete);
            phaseResult.ResourcesZnode = latestResources;
            phaseResult.ClientsZnode = clients;
            
            return phaseResult;
        }

        private async Task<RebalancingPhaseResult> AssignResourcesPhaseAsync(CancellationToken rebalancingToken,
            ResourcesZnode resources,
            ClientsZnode clients)
        {
            logger.Info(this.clientId, "Coordinator - Assign resources to clients");
            var resourcesToAssign = new Queue<string>(resources.Resources);
            var resourceAssignments = new List<ResourceAssignment>();
            var clientIndex = 0;
            while (resourcesToAssign.Any())
            {
                resourceAssignments.Add(new ResourceAssignment()
                {
                    ClientId = GetClientId(clients.ClientPaths[clientIndex]),
                    Resource = resourcesToAssign.Dequeue()
                });

                clientIndex++;
                if (clientIndex >= clients.ClientPaths.Count)
                    clientIndex = 0;
            }
            
            // write assignments back to resources znode
            resources.ResourceAssignments.Assignments = resourceAssignments;
            this.resourcesVersion = await this.zooKeeperService.SetResourcesAsync(resources);
            
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);
            
            this.status.RebalancingStatus = RebalancingStatus.ResourcesGranted;
            this.status.Version = await this.zooKeeperService.SetStatus(this.status);
            
            var leaderAssignments = resourceAssignments.Where(x => x.ClientId == this.clientId).Select(x => x.Resource).ToList();
            await this.store.InvokeOnStartActionsAsync(this.clientId, "Coordinator", leaderAssignments, rebalancingToken, this.coordinatorToken);
            
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);
            
            var phaseResult = new RebalancingPhaseResult(RebalancingResult.Complete);
            phaseResult.ResourcesZnode = resources;
            phaseResult.ClientsZnode = clients;
            
            return phaseResult;
        }

        private async Task<RebalancingPhaseResult> VerifyStartedPhaseAsync(CancellationToken rebalancingToken)
        {
            logger.Info(this.clientId, "Coordinator - Verify all followers have started");
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);
            
            while (!rebalancingToken.IsCancellationRequested)
            {
                var stopped = await this.zooKeeperService.GetStoppedAsync();
                if (!stopped.Any())
                {
                    break;
                }
                else
                {
                    this.logger.Info(this.clientId, $"Coordinator - {stopped.Count} remaining followers to start");
                    await WaitFor(2000); // try again in 1s
                }
            }
            
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);

            logger.Info(this.clientId, "Coordinator - All followers confirm started");
            //status.RebalancingStatus = RebalancingStatus.StartConfirmed;
            //this.status.Version = await this.zooKeeperService.SetStatus(status);
            
            return new RebalancingPhaseResult(RebalancingResult.Complete);
        }
        
        private bool IsRebalancingRequired(ClientsZnode clients, ResourcesZnode resources)
        {
            // if this is the first rebalancing as coordinator or the last one was not successful then rebalancing is required
            if (this.store.AssignmentStatus == AssignmentStatus.NoAssignmentYet 
                || !lastRebalancingResult.HasValue 
                || (lastRebalancingResult.Value != RebalancingResult.Complete && lastRebalancingResult.Value != RebalancingResult.NotRequired))
            {
                return true;
            }

            // any change to resources requires a rebalancing
            if (resources.HasResourceChange())
                return true;

            // given a client was either added or removed
            
            // if there are less clients than resources then we require a rebalancing
            if (clients.ClientPaths.Count < resources.Resources.Count)
                return true;
            
            // given we have an equal or greater number clients than resources
            
            // if an existing client is currently assigned more than one resource we require a rebalancing
            if (resources.ResourceAssignments.Assignments.GroupBy(x => x.ClientId).Any(x => x.Count() > 1))
                return true;
            
            // given all existing assignments are one client to one resource
            
            // if any client for the existing assignments is no longer around then we require a rebalancing
            var clientIds = clients.ClientPaths.Select(GetClientId).ToList();
            foreach (var assignment in resources.ResourceAssignments.Assignments)
            {
                if (!clientIds.Contains(assignment.ClientId, StringComparer.Ordinal))
                    return true;
            }

            // otherwise no rebalancing is required
            return false;
        }
        
        private bool IsClientListMatch(List<string> clientPaths, List<string> stoppedPaths)
        {
            var clientIds = clientPaths.Select(x => GetClientId(x)).Where(x => !x.Equals(this.clientId)).OrderBy(x => x);
            var stoppedClientIds = stoppedPaths.Select(x => GetClientId(x)).OrderBy(x => x);

            return clientIds.SequenceEqual(stoppedClientIds);
        }

        
        
        
    }
}