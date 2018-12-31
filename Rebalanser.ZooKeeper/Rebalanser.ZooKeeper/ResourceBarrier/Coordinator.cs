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

namespace Rebalanser.ZooKeeper.ResourceBarrier
{
    public class Coordinator : Watcher, ICoordinator
    {
        // services
        private IZooKeeperService zooKeeperService;
        private ILogger logger;
        private ResourceManager store;
        
        // immutable state
        private readonly TimeSpan rebalancingTimeLimit;
        private readonly CancellationToken coordinatorToken;
        private readonly string clientId;
        private readonly TimeSpan minimumRebalancingInterval;
        
        // mutable state
        private Task rebalancingTask;
        private CancellationTokenSource rebalancingCts;
        private int resourcesVersion;
        private BlockingCollection<CoordinatorEvent> events;
        private bool ignoreWatches;

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
            this.rebalancingTimeLimit = TimeSpan.FromSeconds(120);
            this.clientId = clientId;
            this.minimumRebalancingInterval = minimumRebalancingInterval;
            this.coordinatorToken = coordinatorToken;
            
            this.rebalancingCts = new CancellationTokenSource();
            this.events = new BlockingCollection<CoordinatorEvent>();
        }
        
        // very important that this method does not throw any exceptions as it is called from the zookeeper library
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
                                rebalancingTask = Task.Run(async () => await TriggerRebalancing(this.rebalancingCts.Token, 1));
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
        
        private async Task TriggerRebalancing(CancellationToken rebalancingToken, int attempt)
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
                }
            }
            catch (ZkSessionExpiredException e)
            {
                this.logger.Error(this.clientId, "Coordinator - The current session has expired", e);
                this.events.Add(CoordinatorEvent.SessionExpired);
            }
            catch (ZkStaleVersionException e)
            {
                this.logger.Error(this.clientId,
                    "Coordinator - A stale znode version was used, aborting rebalancing.", e);
                this.events.Add(CoordinatorEvent.NoLongerCoordinator);
            }
            catch (ZkInvalidOperationException e)
            {
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
                this.logger.Error(this.clientId,
                    "Coordinator - A fatal error has occurred, aborting rebalancing.",
                    e);
                await this.store.SafeInvokeOnErrorActionsAsync(this.clientId, "Client error", e);
                this.events.Add(CoordinatorEvent.FatalError);
            }
            catch (ZkOperationCancelledException)
            {
                logger.Info(this.clientId, "Coordinator - Rebalancing cancelled");
            }
            catch (Exception e)
            {
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
            var sw = new Stopwatch();
            sw.Start();
            
            logger.Info(this.clientId, "Coordinator - Get clients and resources list");
            var clients = await this.zooKeeperService.GetActiveClientsAsync();
            var resources = await this.zooKeeperService.GetResourcesAsync(null, null);
            
            if (resources.Version != this.resourcesVersion)
                throw new ZkStaleVersionException("Resources znode version does not match expected value, indicates another client has been made coordinator and is executing a rebalancing.");

            if (rebalancingToken.IsCancellationRequested) 
                return RebalancingResult.Cancelled;
            
            logger.Info(this.clientId, $"Coordinator - Assign resources ({string.Join(",", resources.Resources)}) to clients ({string.Join(",", clients.ClientPaths)})");
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
                return RebalancingResult.Cancelled;
            
            await this.store.InvokeOnStopActionsAsync(this.clientId, "Coordinator");
            if (rebalancingToken.IsCancellationRequested) 
                return RebalancingResult.Cancelled;
            
            var leaderAssignments = resourceAssignments
                    .Where(x => x.ClientId == this.clientId)
                    .Select(x => x.Resource)
                    .ToList();
            await this.store.InvokeOnStartActionsAsync(this.clientId, "Coordinator", leaderAssignments, rebalancingToken, this.coordinatorToken);
            if (rebalancingToken.IsCancellationRequested) 
                return RebalancingResult.Cancelled;
            
            return RebalancingResult.Complete;
        }

        private string GetClientId(string clientPath)
        {
            return clientPath.Substring(clientPath.LastIndexOf("/", StringComparison.Ordinal)+1);
        }
        
//        private async Task<bool> TryPutResourceBarrierAsync(string resource, Stopwatch sw, CancellationToken token)
//        {
//            var cts = new CancellationTokenSource();
//            var t = Task.Run(async () => await this.zooKeeperService.TryPutResourceBarrierAsync(resource, cts.Token));
//            while (!token.IsCancellationRequested)
//            {
//                if (t.IsCompleted)
//                {
//                    try
//                    {
//                        await t;
//                        return true;
//                    }
//                    catch (Exception e)
//                    {
//                        this.logger.Error("Failed to put resource barrier", e);
//                        return false;
//                    }
//                }
//                else
//                {
//                    if (sw.Elapsed > this.rebalancingTimeLimit)
//                    {
//                        cts.Cancel();
//                        return false;
//                    }
//
//                    await WaitFor(100);
//                }
//            }
//
//            return false;
//        }
    }
}