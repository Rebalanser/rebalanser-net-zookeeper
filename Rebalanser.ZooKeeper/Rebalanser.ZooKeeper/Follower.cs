using System;
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
    public class Follower : Watcher
    {
        private IZooKeeperService zooKeeperService;
        private ILogger logger;
        private ResourceGroupStore store;
        private string clientId;
        private int clientNumber;
        private OnChangeActions onChangeActions;
        private CancellationToken followerToken;
        private FollowerExitReason eventExitReason;
        private bool statusChange;
        private string watchSiblingPath;
        
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
            this.followerToken = followerToken;
        }

        public async Task<bool> InitializeAsync()
        {
            var watchSiblingRes = await this.zooKeeperService.WatchSiblingNodeAsync(this.watchSiblingPath, this);
            if (watchSiblingRes != ZkResult.Ok)
                return false;
            
            var watchStatusRes = await this.zooKeeperService.WatchStatusAsync(this);
            if (watchStatusRes.Result != ZkResult.Ok)
                return false;
            
            if (watchStatusRes.Data.CoordinatorStatus == CoordinatorStatus.ResourcesGranted)
                statusChange = true;

            return true;
        }
        
        public override async Task process(WatchedEvent @event)
        {
            if (@event.getState() == Event.KeeperState.Expired)
            {
                this.eventExitReason = FollowerExitReason.SessionExpired;
            }
            // if the sibling client has been removed then this client must either be the new leader
            // or the node needs to monitor the next smallest client
            else if (@event.getPath().EndsWith(this.watchSiblingPath))
            {
                var ok = await CheckForSiblings();
                if(!ok)
                    eventExitReason = FollowerExitReason.UnexpectedFailure;
            }
            // status change
            else if (@event.getPath().EndsWith("status"))
            {
                statusChange = true;
            }
            else
            {
                // log it 
            }

            await Task.Yield();
        }
        
        public async Task<FollowerExitReason> StartEventLoopAsync()
        {
            while (!this.followerToken.IsCancellationRequested)
            {
                if (this.eventExitReason != FollowerExitReason.NoExit)
                {
                    InvokeOnStopActions();
                    return this.eventExitReason;
                }
                
                if (this.statusChange)
                {
                    this.statusChange = false;
                    var watchStatusRes = await this.zooKeeperService.WatchStatusAsync(this);
                    if (watchStatusRes.Result != ZkResult.Ok)
                    {
                        if (watchStatusRes.Result == ZkResult.SessionExpired)
                            return FollowerExitReason.SessionExpired;

                        return FollowerExitReason.UnexpectedFailure;
                    }

                    var status = watchStatusRes.Data;
                    if (status.CoordinatorStatus == CoordinatorStatus.StopActivity)
                    {
                        InvokeOnStopActions();
                        await this.zooKeeperService.SetFollowerAsStopped(this.clientId);
                    }
                    else if (status.CoordinatorStatus == CoordinatorStatus.ResourcesGranted)
                    {
                        var resourcesRes = await this.zooKeeperService.GetResourcesAsync();
                        if (resourcesRes.Result != ZkResult.Ok)
                        {
                            if (watchStatusRes.Result == ZkResult.SessionExpired)
                                return FollowerExitReason.SessionExpired;

                            return FollowerExitReason.UnexpectedFailure;
                        }

                        var resources = resourcesRes.Data;
                        var assignedResources = resources.ResourceAssignments.Assignments
                            .Where(x => x.ClientId.Equals(this.clientId))
                            .Select(x => x.Resource)
                            .ToList();
                        
                        this.store.SetResources(new SetResourcesRequest()
                        {
                            AssignmentStatus = AssignmentStatus.ResourcesAssigned,
                            Resources = assignedResources
                        });
                        
                        InvokeOnStartActions();
                        var startedRes = await this.zooKeeperService.SetFollowerAsStarted(this.clientId);
                        if (startedRes != ZkResult.Ok)
                        {
                            if (watchStatusRes.Result == ZkResult.SessionExpired)
                                return FollowerExitReason.SessionExpired;

                            return FollowerExitReason.UnexpectedFailure;
                        }
                    }
                    else if (status.CoordinatorStatus == CoordinatorStatus.StartConfirmed)
                    {
                        // do nothing
                    }
                    else
                    {
                        // log unexpected status
                    }
                }

                await WaitFor(1000);
            }

            return FollowerExitReason.UnexpectedFailure;
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
                await Task.Delay(5000, this.followerToken);
            }
            catch (TaskCanceledException)
            {}
        }

        private async Task<bool> CheckForSiblings()
        {
            int maxClientNumber = 0;
            string watchChild = string.Empty;
            var clientsRes = await this.zooKeeperService.GetActiveClientsAsync();
            if (clientsRes.Result != ZkResult.Ok)
                return false;
                
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
            {
                eventExitReason = FollowerExitReason.IsNewLeader;
                return false;
            }
            else
            {
                this.watchSiblingPath = watchChild;
                var newWatchRes = await this.zooKeeperService.WatchSiblingNodeAsync(watchChild, this);
                if (newWatchRes != ZkResult.Ok)
                    return false;
                
                return true;
            }
        }
    }
}