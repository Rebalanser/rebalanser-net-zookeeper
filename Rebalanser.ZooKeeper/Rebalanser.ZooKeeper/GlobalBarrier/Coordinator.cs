using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.Store;
using Rebalanser.ZooKeeper.Zk;

namespace Rebalanser.ZooKeeper.GlobalBarrier
{
    public class Coordinator : Watcher, ICoordinator
    {

        private IZooKeeperService zooKeeperService;
        private ILogger logger;
        private ResourceGroupStore store;
        private StatusZnode status;
        private TimeSpan rebalancingTimeLimit;
        private OnChangeActions onChangeActions;
        private Task rebalancingTask;
        private CancellationToken coordinatorToken;
        private CancellationTokenSource rebalancingCts;
        private bool pendingRebalancing;
        private CoordinatorExitReason eventCoordinatorExitReason;
        private string clientId;

        public Coordinator(IZooKeeperService zooKeeperService,
            ILogger logger,
            ResourceGroupStore store,
            OnChangeActions onChangeActions,
            string clientId,
            CancellationToken coordinatorToken)
        {
            this.zooKeeperService = zooKeeperService;
            this.logger = logger;
            this.store = store;
            this.rebalancingTimeLimit = TimeSpan.FromSeconds(120);
            this.onChangeActions = onChangeActions;
            this.clientId = clientId;
            this.coordinatorToken = coordinatorToken;
            this.rebalancingCts = new CancellationTokenSource();
        }

        public async Task<bool> BecomeCoordinatorAsync()
        {
            var watchEpochRes = await this.zooKeeperService.WatchEpochAsync(this);
            if (watchEpochRes.Result != ZkResult.Ok)
                return false;
            
            var watchNodesRes = await this.zooKeeperService.WatchNodesAsync(this);
            if (watchNodesRes != ZkResult.Ok)
                return false;
            
            var watchResourcesRes = await this.zooKeeperService.WatchResourcesAsync(this);
            if (watchResourcesRes != ZkResult.Ok)
                return false;
            
            var getStatusRes = await this.zooKeeperService.GetStatusAsync();
            if (getStatusRes.Result != ZkResult.Ok)
                return false;

            this.status = getStatusRes.Data;
            this.pendingRebalancing = true;
            return true;
        }

        public override async Task process(WatchedEvent @event)
        {
            if (this.coordinatorToken.IsCancellationRequested)
                return;
            
            if (@event.getState() == Event.KeeperState.Expired)
            {
                this.eventCoordinatorExitReason = CoordinatorExitReason.SessionExpired;
            }
            else if (@event.getPath().EndsWith("epoch"))
            {
                this.eventCoordinatorExitReason = CoordinatorExitReason.NotCoordinator;
            }
            else if (@event.getPath().EndsWith("resources"))
            {
                await this.zooKeeperService.WatchResourcesAsync(this);
                pendingRebalancing = true;
            }
            else if(@event.getPath().EndsWith("clients"))
            {
                await this.zooKeeperService.WatchNodesAsync(this);
                pendingRebalancing = true;
            }
            else
            {
                // log it 
            }

            await Task.Yield();
        }

        public async Task<CoordinatorExitReason> StartEventLoopAsync()
        {
            int logicalTimeSinceRebalancingTrigger = 0;
            while (!this.coordinatorToken.IsCancellationRequested)
            {
                logicalTimeSinceRebalancingTrigger++;
                
                if (this.eventCoordinatorExitReason != CoordinatorExitReason.NoExit)
                {
                    await CancelRebalancingIfInProgressAsync();
                    return this.eventCoordinatorExitReason;
                }
                
                // if a rebalancing event has occurred, delay the start of rebalancing a little
                // prevents multiple close together events causing multiple aborted rebalancings
                if (logicalTimeSinceRebalancingTrigger > 5 && pendingRebalancing)
                {
                    await CancelRebalancingIfInProgressAsync();
                    logger.Info("Rebalancing triggered");
                    pendingRebalancing = false;
                    logicalTimeSinceRebalancingTrigger = 0;
                    rebalancingTask = Task.Run(async () => await TriggerRebalancing(this.rebalancingCts.Token, 1));
                }
                
                await WaitFor(1000);
            }

            if (this.coordinatorToken.IsCancellationRequested)
            {
                await this.zooKeeperService.CloseSessionAsync();
                return CoordinatorExitReason.Cancelled;
            }

            return CoordinatorExitReason.Unknown; // if this happens then we have a correctness bug
        }

        private async Task CancelRebalancingIfInProgressAsync()
        {
            if (this.rebalancingTask != null && !this.rebalancingTask.IsCompleted)
            {
                logger.Info("Cancelling the rebalancing that is in progress");
                this.rebalancingCts.Cancel();
                await this.rebalancingTask; // might need to put a time limit on this
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
                var result = await RebalanceAsync(rebalancingToken);
                switch (result)
                {
                    case RebalancingResult.Complete: 
                        logger.Info("Rebalancing complete");
                        break;
                    case RebalancingResult.Cancelled:
                        logger.Info("Rebalancing cancelled");
                        break;
                    case RebalancingResult.NotCoordinator:
                        logger.Info("Rebalancing aborted, lost coordinator role");
                        this.eventCoordinatorExitReason = CoordinatorExitReason.NotCoordinator;
                        break;
                    case RebalancingResult.SessionExpired:
                        logger.Info("Rebalancing aborted, lost session");
                        this.eventCoordinatorExitReason = CoordinatorExitReason.SessionExpired;
                        break;
                    case RebalancingResult.Failure:
                    case RebalancingResult.TimeLimitExceeded:
                        attempt++;
                        if (attempt > 3)
                        {
                            this.eventCoordinatorExitReason = CoordinatorExitReason.RebalancingError;
                        }
                        else
                        {
                            this.logger.Error($"Rebalancing failed. Will retry again with attempt {attempt}");
                            await TriggerRebalancing(rebalancingToken, attempt);
                        }

                        break;
                    default: 
                        this.logger.Error($"A non-supported RebalancingResult has been returned: {result}");
                        this.eventCoordinatorExitReason = CoordinatorExitReason.RebalancingError;
                        break;
                }
            }
            catch (Exception ex)
            {
                this.logger.Error("An unexpected error has occurred, aborting rebalancing and becoming a follower", ex);
                this.eventCoordinatorExitReason = CoordinatorExitReason.RebalancingError;
            }
        }

        private async Task<RebalancingResult> RebalanceAsync(CancellationToken rebalancingToken)
        {
            var sw = new Stopwatch();
            sw.Start();

            var stopPhaseResult = await StopActivityPhaseAsync(rebalancingToken, sw);
            if (stopPhaseResult.PhaseResult != RebalancingResult.Complete)
                return stopPhaseResult.PhaseResult;
            
            var assignPhaseResult = await AssignResourcesPhaseAsync(rebalancingToken, 
                stopPhaseResult.ResourcesZnode,
                stopPhaseResult.ClientsZnode);
            
            if (assignPhaseResult.PhaseResult != RebalancingResult.Complete)
                return assignPhaseResult.PhaseResult;

            var verifyPhaseResult = await VerifyStartedPhaseAsync(rebalancingToken, sw);
            if (verifyPhaseResult.PhaseResult != RebalancingResult.Complete)
                return assignPhaseResult.PhaseResult;
            
            return RebalancingResult.Complete;
        }

        private async Task<RebalancingPhaseResult> StopActivityPhaseAsync(CancellationToken rebalancingToken, Stopwatch sw)
        {
            logger.Info("PHASE 1 - Command followers to stop");
            status.CoordinatorStatus = CoordinatorStatus.StopActivity;
            var setStatusRes = await this.zooKeeperService.SetStatus(status);
            if (setStatusRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.BadVersion)
                    return new RebalancingPhaseResult(RebalancingResult.NotCoordinator);
                
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return new RebalancingPhaseResult(RebalancingResult.SessionExpired);

                return new RebalancingPhaseResult(RebalancingResult.Failure);
            }

            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);

            status.Version = setStatusRes.Data;
            InvokeOnStopActions();
            
            var clientsRes = await this.zooKeeperService.GetActiveClientsAsync();
            if (clientsRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return new RebalancingPhaseResult(RebalancingResult.SessionExpired);
                
                return new RebalancingPhaseResult(RebalancingResult.Failure);
            }

            var clients = clientsRes.Data;
            
            var resourcesRes = await this.zooKeeperService.GetResourcesAsync();
            if (resourcesRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return new RebalancingPhaseResult(RebalancingResult.SessionExpired);
                
                return new RebalancingPhaseResult(RebalancingResult.Failure);
            }
            var resources = resourcesRes.Data;
            
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);
            
            // wait for confirmation that all followers have stopped or for time limit
            bool allActivityStopped = false;
            while (!allActivityStopped && sw.Elapsed < this.rebalancingTimeLimit && !rebalancingToken.IsCancellationRequested)
            {
                var stoppedRes = await this.zooKeeperService.GetStoppedAsync();
                if(stoppedRes.Result == ZkResult.Ok)
                    allActivityStopped = IsClientListMatch(clients.ClientPaths, stoppedRes.Data);
                else if (stoppedRes.Result == ZkResult.SessionExpired)
                    return new RebalancingPhaseResult(RebalancingResult.SessionExpired);
                else
                    await WaitFor(1000); // try again in 1s
            }

            if (sw.Elapsed > this.rebalancingTimeLimit)
            {
                logger.Error($"Rebalancing aborted, exceeded time limit of {this.rebalancingTimeLimit}");
                return new RebalancingPhaseResult(RebalancingResult.TimeLimitExceeded);
            }

            var phaseResult = new RebalancingPhaseResult(RebalancingResult.Complete);
            phaseResult.ResourcesZnode = resources;
            phaseResult.ClientsZnode = clients;
            
            return phaseResult;
        }

        private async Task<RebalancingPhaseResult> AssignResourcesPhaseAsync(CancellationToken rebalancingToken,
            ResourcesZnode resources,
            ClientsZnode clients)
        {
            logger.Info("PHASE 2 - Assign resources to clients");
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
            var setResourcesRes = await this.zooKeeperService.SetResourcesAsync(resources);
            if (setResourcesRes.Result != ZkResult.Ok)
            {
                if (setResourcesRes.Result == ZkResult.BadVersion)
                    return new RebalancingPhaseResult(RebalancingResult.NotCoordinator);
                if (setResourcesRes.Result == ZkResult.SessionExpired)
                    return new RebalancingPhaseResult(RebalancingResult.SessionExpired);

                return new RebalancingPhaseResult(RebalancingResult.Failure);
            }
            
            resources.Version = setResourcesRes.Data;
            
            // set status to ResourcesGranted
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);
            
            this.status.CoordinatorStatus = CoordinatorStatus.ResourcesGranted;
            var setStatusRes = await this.zooKeeperService.SetStatus(this.status);
            if (setStatusRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.BadVersion)
                    return new RebalancingPhaseResult(RebalancingResult.NotCoordinator);
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return new RebalancingPhaseResult(RebalancingResult.SessionExpired);

                return new RebalancingPhaseResult(RebalancingResult.Failure);
            }

            this.status.Version = setStatusRes.Data;
            
            var leaderAssignments = resourceAssignments.Where(x => x.ClientId == this.clientId).ToList();
            this.store.SetResources(new SetResourcesRequest()
            {
                AssignmentStatus = AssignmentStatus.ResourcesAssigned, 
                Resources = leaderAssignments.Select(x => x.Resource).ToList()
            });
            InvokeOnStartActions();
            
            var phaseResult = new RebalancingPhaseResult(RebalancingResult.Complete);
            phaseResult.ResourcesZnode = resources;
            phaseResult.ClientsZnode = clients;
            
            return phaseResult;
        }

        private async Task<RebalancingPhaseResult> VerifyStartedPhaseAsync(CancellationToken rebalancingToken,
            Stopwatch sw)
        {
            logger.Info("PHASE 3 - Verify all followers have started");
            if (rebalancingToken.IsCancellationRequested) 
                return new RebalancingPhaseResult(RebalancingResult.Cancelled);
            
            bool allActivityStarted = false;
            while (!allActivityStarted || sw.Elapsed > this.rebalancingTimeLimit)
            {
                var stoppedRes = await this.zooKeeperService.GetStoppedAsync();
                if(stoppedRes.Result == ZkResult.Ok)
                    allActivityStarted = !stoppedRes.Data.Any();    
                else if (stoppedRes.Result == ZkResult.SessionExpired)
                    return new RebalancingPhaseResult(RebalancingResult.SessionExpired);
                else
                    await WaitFor(1000); // try again in 1s
            }

            if (sw.Elapsed > this.rebalancingTimeLimit)
            {
                logger.Error($"Rebalancing aborted, exceeded time limit of {this.rebalancingTimeLimit}");
                return new RebalancingPhaseResult(RebalancingResult.TimeLimitExceeded);
            }

            status.CoordinatorStatus = CoordinatorStatus.StartConfirmed;
            var setStatusRes = await this.zooKeeperService.SetStatus(status);
            if (setStatusRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.BadVersion)
                    return new RebalancingPhaseResult(RebalancingResult.NotCoordinator);
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return new RebalancingPhaseResult(RebalancingResult.SessionExpired);

                return new RebalancingPhaseResult(RebalancingResult.Failure);
            }
            
            this.status.Version = setStatusRes.Data;
            return new RebalancingPhaseResult(RebalancingResult.Complete);
        }
        
        private bool IsClientListMatch(List<string> clientPaths, List<string> stoppedPaths)
        {
            var clientIds = clientPaths.Select(x => GetClientId(x)).Where(x => !x.Equals(this.clientId)).OrderBy(x => x);
            var stoppedClientIds = stoppedPaths.Select(x => GetClientId(x)).OrderBy(x => x);

            return clientIds.SequenceEqual(stoppedClientIds);
        }

        private string GetClientId(string clientPath)
        {
            return clientPath.Substring(clientPath.LastIndexOf("/", StringComparison.Ordinal)+1);
        }
        
        private void InvokeOnStopActions()
        {
            foreach(var onStopAction in this.onChangeActions.OnStopActions)
                onStopAction.Invoke();
        }
        
        private void InvokeOnStartActions()
        {
            foreach(var onStartAction in this.onChangeActions.OnStartActions)
                onStartAction.Invoke();
        }
    }
}