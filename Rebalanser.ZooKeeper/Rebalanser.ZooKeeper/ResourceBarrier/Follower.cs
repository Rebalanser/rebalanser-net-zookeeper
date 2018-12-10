using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.GlobalBarrier;
using Rebalanser.ZooKeeper.Store;
using Rebalanser.ZooKeeper.Zk;

namespace Rebalanser.ZooKeeper.ResourceBarrier
{
    public class Follower : Watcher, IFollower
    {
        private IZooKeeperService zooKeeperService;
        private ILogger logger;
        private ResourceGroupStore store;
        private string clientId;
        private int clientNumber;
        private OnChangeActions onChangeActions;
        private CancellationToken followerToken;
        private FollowerStatus followerStatus;
        private bool statusChange;
        private string watchSiblingPath;
        private string siblingId;
        private Task rebalancingTask;
        private CancellationTokenSource rebalancingCts;
        

        private enum SiblingCheckResult
        {
            WatchingNewSibling,
            IsNewLeader,
            Error
        }
        
        public Follower(IZooKeeperService zooKeeperService,
            ILogger logger,
            ResourceGroupStore store,
            OnChangeActions onChangeActions,
            string clientId,
            int clientNumber,
            string watchSiblingPath,
            CancellationToken followerToken)
        {
            this.zooKeeperService = zooKeeperService;
            this.logger = logger;
            this.store = store;
            this.onChangeActions = onChangeActions;
            this.clientId = clientId;
            this.clientNumber = clientNumber;
            this.watchSiblingPath = watchSiblingPath;
            this.siblingId = watchSiblingPath.Substring(watchSiblingPath.LastIndexOf("/", StringComparison.Ordinal));
            this.followerToken = followerToken;
            
            this.rebalancingCts = new CancellationTokenSource();
        }

        public async Task<bool> BecomeFollowerAsync()
        {
            var watchSiblingRes = await this.zooKeeperService.WatchSiblingNodeAsync(this.watchSiblingPath, this);
            if (watchSiblingRes != ZkResult.Ok)
            {
                if(watchSiblingRes == ZkResult.NoZnode)
                    this.logger.Info($"Could not set a watch on sibling node {this.watchSiblingPath} as it no longer exists");
                return false;
            }

            var watchStatusRes = await this.zooKeeperService.WatchResourcesDataAsync(this);
            if (watchStatusRes != ZkResult.Ok)
                return false;
            
            return true;
        }
        
        public override async Task process(WatchedEvent @event)
        {
            if (@event.getState() == Event.KeeperState.Expired)
            {
                this.followerStatus = FollowerStatus.SessionExpired;
            }
            // if the sibling client has been removed then this client must either be the new leader
            // or the node needs to monitor the next smallest client
            else if (@event.getPath().EndsWith(this.siblingId))
            {
                var siblingResult = await CheckForSiblings();
                switch (siblingResult)
                {
                    case SiblingCheckResult.WatchingNewSibling:
                        break;
                    case SiblingCheckResult.IsNewLeader:
                        followerStatus = FollowerStatus.IsNewLeader;
                        break;
                    case SiblingCheckResult.Error:
                        followerStatus = FollowerStatus.UnexpectedFailure;
                        break;
                    default:
                        this.logger.Error($"Non-supported SiblingCheckResult {siblingResult}");
                        break;
                }
            }
            // status change
            else if (@event.getPath().EndsWith("resources"))
            {
                var watchStatusRes = await this.zooKeeperService.WatchResourcesDataAsync(this);
                if (watchStatusRes != ZkResult.Ok)
                    this.followerStatus = FollowerStatus.UnexpectedFailure;
                else
                    statusChange = true;
            }
            else
            {
                // log it 
            }

            await Task.Yield();
        }
        
        public async Task<FollowerStatus> StartEventLoopAsync()
        {
            while (!this.followerToken.IsCancellationRequested)
            {
                if (this.followerStatus != FollowerStatus.Ok)
                {
                    await CancelRebalancingIfInProgressAsync();
                    InvokeOnStopActions();
                    return this.followerStatus;
                }
                
                if (this.statusChange)
                {
                    await CancelRebalancingIfInProgressAsync();
                    logger.Info("Rebalancing triggered");
                    this.statusChange = false;
                    rebalancingTask = Task.Run(async () => await RespondToRebalancing(this.rebalancingCts.Token, 1));
                }

                await WaitFor(1000);
            }

            if (this.followerToken.IsCancellationRequested)
            {
                await this.zooKeeperService.CloseSessionAsync();
                return FollowerStatus.Cancelled;
            }

            return FollowerStatus.UnexpectedFailure;
        }
        
        private async Task RespondToRebalancing(CancellationToken rebalancingToken, int attempt)
        {
            try
            {
                var result = await ProcessStatusChangeAsync(rebalancingToken);
                switch (result)
                {
                    case RebalancingResult.Complete: 
                        logger.Info("Rebalancing complete");
                        break;
                    case RebalancingResult.Cancelled:
                        logger.Info("Rebalancing cancelled");
                        break;
                    case RebalancingResult.SessionExpired:
                        logger.Info("Rebalancing aborted, lost session");
                        this.followerStatus = FollowerStatus.SessionExpired;
                        break;
                    case RebalancingResult.Failure:
                        attempt++;
                        if (attempt > 3)
                        {
                            this.followerStatus = FollowerStatus.UnexpectedFailure;
                        }
                        else
                        {
                            this.logger.Error($"Rebalancing failed. Will retry again with attempt {attempt}");
                            await RespondToRebalancing(rebalancingToken, attempt);
                        }

                        break;
                    default: 
                        this.logger.Error($"A non-supported RebalancingResult has been returned: {result}");
                        this.followerStatus = FollowerStatus.UnexpectedFailure;
                        break;
                }
            }
            catch (Exception ex)
            {
                this.logger.Error("An unexpected error has occurred, aborting rebalancing", ex);
                this.followerStatus = FollowerStatus.UnexpectedFailure;
            }
        }

        private async Task<RebalancingResult> ProcessStatusChangeAsync(CancellationToken rebalancingToken)
        {
            this.statusChange = false;
        
            var resourcesRes = await this.zooKeeperService.GetResourcesAsync();
            if (resourcesRes.Result != ZkResult.Ok)
            {
                if (resourcesRes.Result == ZkResult.SessionExpired)
                    return RebalancingResult.SessionExpired;
                
                return RebalancingResult.Failure;
            }
            
            var currAssignmentRes = this.store.GetResources();
            foreach (var resource in currAssignmentRes.Resources)
            {
                var removeBarrierRes = await this.zooKeeperService.RemoveResourceBarrierAsync(resource);
                if(removeBarrierRes != ZkResult.Ok)
                    return RebalancingResult.Failure;
            }
            
            var resources = resourcesRes.Data;
            var assignedResources = resources.ResourceAssignments.Assignments
                .Where(x => x.ClientId.Equals(this.clientId))
                .Select(x => x.Resource)
                .ToList();

            foreach (var newResource in assignedResources)
            {
                var barrierAdded = await TryPutResourceBarrierAsync(newResource, rebalancingToken);
                if (!barrierAdded)
                {
                    if (rebalancingToken.IsCancellationRequested)
                        return RebalancingResult.Cancelled;
                }
            }
            
            this.store.SetResources(new SetResourcesRequest()
            {
                AssignmentStatus = AssignmentStatus.ResourcesAssigned, 
                Resources = assignedResources
            });
            InvokeOnStartActions();
            
            return RebalancingResult.Complete;
        }
        
        private async Task<bool> TryPutResourceBarrierAsync(string resource, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
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
        
        private async Task WaitFor(int milliseconds)
        {
            try
            {
                await Task.Delay(milliseconds, this.followerToken);
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

        private async Task<SiblingCheckResult> CheckForSiblings()
        {
            int maxClientNumber = 0;
            string watchChild = string.Empty;
            var clientsRes = await this.zooKeeperService.GetActiveClientsAsync();
            if (clientsRes.Result != ZkResult.Ok)
                return SiblingCheckResult.Error;
                
            var clients = clientsRes.Data;
            foreach (var childPath in clients.ClientPaths)
            {
                int siblingClientNumber = int.Parse(childPath.Substring(childPath.Length - 10, 10));
                if (siblingClientNumber > maxClientNumber && siblingClientNumber < this.clientNumber)
                {
                    watchChild = childPath;
                    maxClientNumber = siblingClientNumber;
                }
            }

            if (maxClientNumber == 0)
                return SiblingCheckResult.IsNewLeader;
            
            this.watchSiblingPath = watchChild;
            this.siblingId = watchSiblingPath.Substring(watchChild.LastIndexOf("/", StringComparison.Ordinal));
            var newWatchRes = await this.zooKeeperService.WatchSiblingNodeAsync(watchChild, this);
            if (newWatchRes != ZkResult.Ok)
                return SiblingCheckResult.Error;
            
            return SiblingCheckResult.WatchingNewSibling;
        }
    }
}