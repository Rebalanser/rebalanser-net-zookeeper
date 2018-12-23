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

namespace Rebalanser.ZooKeeper.ResourceBarrier
{
    public class Coordinator : Watcher, ICoordinator
    {
        private IZooKeeperService zooKeeperService;
        private ILogger logger;
        private ResourceGroupStore store;
        private TimeSpan rebalancingTimeLimit;
        private OnChangeActions onChangeActions;
        private Task rebalancingTask;
        private CancellationToken coordinatorToken;
        private CancellationTokenSource rebalancingCts;
        private bool pendingRebalancing;
        private CoordinatorExitReason eventCoordinatorExitReason;
        private string clientId;
        private int resourcesVersion;

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
        
        public override async Task process(WatchedEvent @event)
        {
            if (this.coordinatorToken.IsCancellationRequested)
                return;

            switch (@event.getState())
            {
                case Event.KeeperState.Expired:
                    this.eventCoordinatorExitReason = CoordinatorExitReason.SessionExpired;
                    break;
                case Event.KeeperState.Disconnected:
                    break;
                case Event.KeeperState.SyncConnected:
                case Event.KeeperState.ConnectedReadOnly:
                    if (@event.getPath() != null)
                    {
                        if (@event.getPath().EndsWith("epoch"))
                        {
                            this.eventCoordinatorExitReason = CoordinatorExitReason.NotCoordinator;
                        }
                        else if (@event.getPath().EndsWith("resources"))
                        {
                            await this.zooKeeperService.WatchResourcesChildrenAsync(this);
                            pendingRebalancing = true;
                        }
                        else if(@event.getPath().EndsWith("clients"))
                        {
                            await this.zooKeeperService.WatchNodesAsync(this);
                            pendingRebalancing = true;
                        }
                    }

                    break;
                default:
                    this.eventCoordinatorExitReason = CoordinatorExitReason.Unknown;
                    this.logger.Error(this.clientId,
                        $"Coordinator - Currently this library does not support ZooKeeper state {@event.getState()}");
                    break;
            }

            await Task.Yield();
        }
        
        public async Task<BecomeCoordinatorResult> BecomeCoordinatorAsync(int currentEpoch)
        {
            var incEpochRes = await this.zooKeeperService.IncrementEpochAsync(currentEpoch);
            if (incEpochRes.Result != ZkResult.Ok)
            {
                if(incEpochRes.Result == ZkResult.BadVersion)
                    return BecomeCoordinatorResult.StaleEpoch;

                return BecomeCoordinatorResult.Error;
            }
            
            var watchEpochRes = await this.zooKeeperService.WatchEpochAsync(this);
            if (watchEpochRes.Result != ZkResult.Ok)
                return BecomeCoordinatorResult.Error;
            
            if(watchEpochRes.Data != incEpochRes.Data)
                return BecomeCoordinatorResult.StaleEpoch;
            
            var watchNodesRes = await this.zooKeeperService.WatchNodesAsync(this);
            if (watchNodesRes != ZkResult.Ok)
                return BecomeCoordinatorResult.Error;
            
            var watchResourcesRes = await this.zooKeeperService.WatchResourcesChildrenAsync(this);
            if (watchResourcesRes != ZkResult.Ok)
                return BecomeCoordinatorResult.Error;

            var getResourcesRes = await this.zooKeeperService.GetResourcesAsync();
            if (getResourcesRes.Result != ZkResult.Ok)
                return BecomeCoordinatorResult.Error;
            
            this.resourcesVersion = getResourcesRes.Data.Version;
            
            this.pendingRebalancing = true;
            return BecomeCoordinatorResult.Ok;
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
                    logger.Info(this.clientId, "Coordinator - Rebalancing triggered");
                    pendingRebalancing = false;
                    logicalTimeSinceRebalancingTrigger = 0;
                    rebalancingTask = Task.Run(async () => await TriggerRebalancing(this.rebalancingCts.Token, 1));
                }
                
                await WaitFor(1000);
            }

            if (this.coordinatorToken.IsCancellationRequested)
            {
                await CancelRebalancingIfInProgressAsync();
                await this.zooKeeperService.CloseSessionAsync();
                return CoordinatorExitReason.Cancelled;
            }

            return CoordinatorExitReason.Unknown; // if this happens then we have a correctness bug
        }

        private async Task CancelRebalancingIfInProgressAsync()
        {
            if (this.rebalancingTask != null && !this.rebalancingTask.IsCompleted)
            {
                logger.Info(this.clientId, "Coordinator - Cancelling the rebalancing that is in progress");
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
        
        private async Task WaitFor(int milliseconds, CancellationToken token)
        {
            try
            {
                await Task.Delay(milliseconds, token);
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
                        logger.Info(this.clientId, "Coordinator - Rebalancing complete");
                        break;
                    case RebalancingResult.Cancelled:
                        logger.Info(this.clientId, "Coordinator - Rebalancing cancelled");
                        break;
                    case RebalancingResult.NotCoordinator:
                        logger.Info(this.clientId, "Coordinator - Rebalancing aborted, lost coordinator role");
                        this.eventCoordinatorExitReason = CoordinatorExitReason.NotCoordinator;
                        break;
                    case RebalancingResult.SessionExpired:
                        logger.Info(this.clientId, "Coordinator - Rebalancing aborted, lost session");
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
                            this.logger.Error(this.clientId, $"Coordinator - Rebalancing failed. Will retry again with attempt {attempt}");
                            await TriggerRebalancing(rebalancingToken, attempt);
                        }

                        break;
                    default: 
                        this.logger.Error(this.clientId, $"Coordinator - A non-supported RebalancingResult has been returned: {result}");
                        this.eventCoordinatorExitReason = CoordinatorExitReason.RebalancingError;
                        break;
                }
            }
            catch (Exception ex)
            {
                this.logger.Error(this.clientId, "Coordinator - An unexpected error has occurred, aborting rebalancing and becoming a follower", ex);
                this.eventCoordinatorExitReason = CoordinatorExitReason.RebalancingError;
            }
        }

        private async Task<RebalancingResult> RebalanceAsync(CancellationToken rebalancingToken)
        {
            var sw = new Stopwatch();
            sw.Start();
            
            logger.Info(this.clientId, "Coordinator - PHASE 1 - Get clients and resources list");
            var clientsRes = await this.zooKeeperService.GetActiveClientsAsync();
            if (clientsRes.Result != ZkResult.Ok)
            {
                if (clientsRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;
                
                return RebalancingResult.Failure;
            }

            var clients = clientsRes.Data;
            
            var resourcesRes = await this.zooKeeperService.GetResourcesAsync();
            if (resourcesRes.Result != ZkResult.Ok)
            {
                if (resourcesRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;
                
                return RebalancingResult.Failure;
            }
            
            var resources = resourcesRes.Data;
            if (resources.Version != this.resourcesVersion)
            {
                this.logger.Error(this.clientId, "Coordinator - Resources znode version does not match expected value, indicates another client has been made coordinator and is executing a rebalancing. Stopping being coordinator now.");
                return RebalancingResult.NotCoordinator;
            }

            if (rebalancingToken.IsCancellationRequested) 
                return RebalancingResult.Cancelled;
            
            logger.Info(this.clientId, $"Coordinator - PHASE 2 - Assign resources ({string.Join(",", resources.Resources)}) to clients ({string.Join(",", clients.ClientPaths)})");
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
                    return RebalancingResult.NotCoordinator;
                if (setResourcesRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;

                return RebalancingResult.Failure;
            }
            
            this.resourcesVersion = setResourcesRes.Data;
            
            logger.Info(this.clientId, "Coordinator - PHASE 3 - Remove any existing barriers, place new resource barriers and invoke start actions");
            var leaderAssignments = resourceAssignments.Where(x => x.ClientId == this.clientId).ToList();
            var currAssignmentRes = this.store.GetResources();
            InvokeOnStopActions();
            
            foreach (var resource in currAssignmentRes.Resources)
            {
                var removeBarrierRes = await this.zooKeeperService.RemoveResourceBarrierAsync(resource);
                if(removeBarrierRes != ZkResult.Ok)
                    return RebalancingResult.Failure;
            }

            foreach (var newResource in leaderAssignments)
            {
                var barrierAdded = await TryPutResourceBarrierAsync(newResource.Resource, sw, rebalancingToken);
                if (!barrierAdded)
                {
                    if (rebalancingToken.IsCancellationRequested)
                        return RebalancingResult.Cancelled;
                    if (sw.Elapsed > this.rebalancingTimeLimit)
                        return RebalancingResult.TimeLimitExceeded;
                }
            }
            
            this.store.SetResources(new SetResourcesRequest()
            {
                AssignmentStatus = AssignmentStatus.ResourcesAssigned, 
                Resources = leaderAssignments.Select(x => x.Resource).ToList()
            });
            InvokeOnStartActions(leaderAssignments.Select(x => x.Resource).ToList());
            
            return RebalancingResult.Complete;
        }

        private async Task<bool> TryPutResourceBarrierAsync(string resource, Stopwatch sw, CancellationToken token)
        {
            while (!token.IsCancellationRequested && sw.Elapsed < this.rebalancingTimeLimit)
            {
                var putBarrierRes = await this.zooKeeperService.TryPutResourceBarrierAsync(resource);
                if (putBarrierRes == ZkResult.Ok)
                    return true;

                if (putBarrierRes == ZkResult.NodeAlreadyExists)
                {
                    await WaitFor(1000, token);
                    continue;
                }

                return false;
            }

            return false;
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
        
        private void InvokeOnStartActions(List<string> assignedResources)
        {
            foreach(var onStartAction in this.onChangeActions.OnStartActions)
                onStartAction.Invoke(assignedResources);
        }
    }
}