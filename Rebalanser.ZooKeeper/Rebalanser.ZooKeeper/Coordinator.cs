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

namespace Rebalanser.ZooKeeper
{
    public class Coordinator : Watcher
    {

        private IZooKeeperService zooKeeperService;
        private ILogger logger;
        private ResourceGroupStore store;
        private Event.KeeperState keeperState;
        private StatusZnode status;
        private TimeSpan rebalancingTimeLimit;
        private OnChangeActions onChangeActions;
        private Task rebalancingTask;
        private CancellationToken coordinatorToken;
        private CancellationTokenSource rebalancingCts;
        private bool pendingRebalancing;
        private CoordinatorExitReason eventCoordinatorExitReason;
        private string clientId;
        private int epoch;

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

            this.epoch = watchEpochRes.Data;
            
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
            return true;
        }

        public override async Task process(WatchedEvent @event)
        {
            this.keeperState = @event.getState();
            
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
            while (!this.coordinatorToken.IsCancellationRequested)
            {
                if (this.eventCoordinatorExitReason != CoordinatorExitReason.NoExit)
                {
                    await WaitForRebalancingToEnd();
                    return this.eventCoordinatorExitReason;
                }
                
                if (pendingRebalancing)
                {
                    logger.Info("Rebalancing required");
                    if (rebalancingTask == null || rebalancingTask.IsCompleted)
                    {
                        logger.Info("Rebalancing triggered");
                        pendingRebalancing = false;
                        rebalancingTask = Task.Run(async () => await TriggerRebalancing(this.rebalancingCts.Token, 1));
                    }
                    else
                    {
                        logger.Info("Rebalancing already in progress, will wait for current rebalancing to complete");
                        // do nothing, wait for current rebalancing to complete
                    }
                }
                
                await WaitFor(1000);
            }
            
            if(coordinatorToken.IsCancellationRequested)
                return CoordinatorExitReason.Cancelled;

            return CoordinatorExitReason.Unknown; // if this happens then we have a correctness bug
        }

        private async Task WaitForRebalancingToEnd()
        {
            if (this.rebalancingTask != null && !this.rebalancingTask.IsCompleted)
            {
                logger.Info("Cancelling the rebalancing that is in progress");
                this.rebalancingCts.Cancel();
                await this.rebalancingTask; // might need to put a time limit on this
                logger.Info("Rebalancing cancelled");
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
                if (result == RebalancingResult.NotCoordinator)
                {
                    this.eventCoordinatorExitReason = CoordinatorExitReason.NotCoordinator;
                }
                else if(result == RebalancingResult.SessionExpired)
                {
                    this.eventCoordinatorExitReason = CoordinatorExitReason.SessionExpired;
                }
                else if (result == RebalancingResult.Failure || result == RebalancingResult.TimeLimitExceeded)
                {
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
                }
            }
            catch (Exception ex)
            {
                this.logger.Error("An unexpected error has occurred, aborting rebalancing and becoming a follower", ex);
                this.eventCoordinatorExitReason = CoordinatorExitReason.RebalancingError;
            }
        }
        
        /// <summary>
        /// This method is too long and needs to be refactored
        /// </summary>
        /// <param name="rebalancingToken"></param>
        /// <returns></returns>
        private async Task<RebalancingResult> RebalanceAsync(CancellationToken rebalancingToken)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            //======== PHASE 1 - STOP ACTIVITY  ================
            logger.Info("PHASE 1 - STOP ACTIVITY");
            status.CoordinatorStatus = CoordinatorStatus.StopActivity;
            var setStatusRes = await this.zooKeeperService.SetStatus(status);
            if (setStatusRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.BadVersion)
                    return RebalancingResult.NotCoordinator;
                
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;

                return RebalancingResult.Failure;
            }

            if (rebalancingToken.IsCancellationRequested) return RebalancingResult.Cancelled;

            status.Version = setStatusRes.Data;
            InvokeOnStopActions();
            
            var clientsRes = await this.zooKeeperService.GetActiveClientsAsync();
            if (clientsRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;
                
                return RebalancingResult.Failure;
            }

            var clients = clientsRes.Data;
            
            var resourcesRes = await this.zooKeeperService.GetResourcesAsync();
            if (resourcesRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;
                
                return RebalancingResult.Failure;
            }
            var resources = resourcesRes.Data;
            
            if (rebalancingToken.IsCancellationRequested) return RebalancingResult.Cancelled;
            
            // wait for confirmation that all followers have stopped or for time limit
            bool allActivityStopped = false;
            while (!allActivityStopped || sw.Elapsed > this.rebalancingTimeLimit)
            {
                var stoppedRes = await this.zooKeeperService.GetStoppedAsync();
                if(stoppedRes.Result == ZkResult.Ok)
                    allActivityStopped = IsClientListMatch(clients.ClientPaths, stoppedRes.Data);
                else if (stoppedRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;
                else
                    await WaitFor(1000); // try again in 1s
            }

            if (sw.Elapsed > this.rebalancingTimeLimit)
            {
                logger.Error($"Rebalancing aborted, exceeded time limit of {this.rebalancingTimeLimit}");
                return RebalancingResult.TimeLimitExceeded;
            }

            if (rebalancingToken.IsCancellationRequested) return RebalancingResult.Cancelled;
            
            //======== PHASE 2 - ASSIGN RESOURCES TO CLIENTS  ================
            logger.Info("PHASE 2 - ASSIGN RESOURCES TO CLIENTS");
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

                if (clientIndex >= clients.ClientPaths.Count)
                    clientIndex = 0;
            }
            
            // write assignments back to resources znode
            resources.ResourceAssignments.Assignments = resourceAssignments;
            var setResourcesRes = await this.zooKeeperService.SetResourcesAsync(resources);
            if (setResourcesRes.Result != ZkResult.Ok)
            {
                if (setResourcesRes.Result == ZkResult.BadVersion)
                    return RebalancingResult.NotCoordinator;
                if (setResourcesRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;

                return RebalancingResult.Failure;
            }
            
            resources.Version = setResourcesRes.Data;
            
            // set status to ResourcesGranted
            if (rebalancingToken.IsCancellationRequested) return RebalancingResult.Cancelled;
            
            this.status.CoordinatorStatus = CoordinatorStatus.ResourcesGranted;
            setStatusRes = await this.zooKeeperService.SetStatus(this.status);
            if (setStatusRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.BadVersion)
                    return RebalancingResult.NotCoordinator;
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;

                return RebalancingResult.Failure;
            }

            this.status.Version = setStatusRes.Data;

            //======== PHASE 3 - START CONSUMPTION LOCALLY  ================
            logger.Info("PHASE 3 - START CONSUMPTION LOCALLY");
            var leaderAssignments = resourceAssignments.Where(x => x.ClientId == this.clientId).ToList();
            this.store.SetResources(new SetResourcesRequest()
            {
                AssignmentStatus = AssignmentStatus.ResourcesAssigned, 
                Resources = leaderAssignments.Select(x => x.Resource).ToList()
            });
            InvokeOnStartActions();
            
            //======== PHASE 4 - VERIFY ALL FOLLOWERS HAVE STARTED  ================
            logger.Info("PHASE 4 - VERIFY ALL FOLLOWERS HAVE STARTED");
            if (rebalancingToken.IsCancellationRequested) return RebalancingResult.Cancelled;
            bool allActivityStarted = false;
            while (!allActivityStarted || sw.Elapsed > this.rebalancingTimeLimit)
            {
                var stoppedRes = await this.zooKeeperService.GetStoppedAsync();
                if(stoppedRes.Result == ZkResult.Ok)
                    allActivityStarted = !stoppedRes.Data.Any();    
                else if (stoppedRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;
                else
                    await WaitFor(1000); // try again in 1s
            }

            if (sw.Elapsed > this.rebalancingTimeLimit)
            {
                logger.Error($"Rebalancing aborted, exceeded time limit of {this.rebalancingTimeLimit}");
                return RebalancingResult.TimeLimitExceeded;
            }

            status.CoordinatorStatus = CoordinatorStatus.StartConfirmed;
            setStatusRes = await this.zooKeeperService.SetStatus(status);
            
            if (setStatusRes.Result != ZkResult.Ok)
            {
                if (setStatusRes.Result == ZkResult.BadVersion)
                    return RebalancingResult.NotCoordinator;
                if (setStatusRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;

                return RebalancingResult.Failure;
            }
            status.Version = setStatusRes.Data;
            
            // =============== COMPLETE =======================
            logger.Info("rebalancing complete");
            return RebalancingResult.Complete;
        }

        private bool IsClientListMatch(List<string> paths1, List<string> paths2)
        {
            var clientIds1 = paths1.Select(x => GetClientId(x)).OrderBy(x => x);
            var clientIds2 = paths2.Select(x => GetClientId(x)).OrderBy(x => x);

            return clientIds1.SequenceEqual(clientIds2);
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