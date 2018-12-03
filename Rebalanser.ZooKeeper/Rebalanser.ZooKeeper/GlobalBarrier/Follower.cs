using System;
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
    public class Follower : Watcher, IFollower
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
        private string siblingId;
        private int statusVersion;

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

            var watchStatusRes = await this.zooKeeperService.WatchStatusAsync(this);
            if (watchStatusRes.Result != ZkResult.Ok)
                return false;
            
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
            else if (@event.getPath().EndsWith(this.siblingId))
            {
                var siblingResult = await CheckForSiblings();
                switch (siblingResult)
                {
                    case SiblingCheckResult.WatchingNewSibling:
                        break;
                    case SiblingCheckResult.IsNewLeader:
                        eventExitReason = FollowerExitReason.IsNewLeader;
                        break;
                    case SiblingCheckResult.Error:
                        eventExitReason = FollowerExitReason.UnexpectedFailure;
                        break;
                    default:
                        this.logger.Error($"Non-supported SiblingCheckResult {siblingResult}");
                        break;
                }
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
            int lastStopVersion = 0;
            int lastStartVersion = 0;
            
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
                        var stoppedRes = await this.zooKeeperService.SetFollowerAsStopped(this.clientId);
                        if (stoppedRes != ZkResult.Ok)
                        {
                            if (stoppedRes == ZkResult.NodeAlreadyExists && lastStopVersion > lastStartVersion)
                                this.logger.Info("Two consecutive stop commands received.");
                            else if (stoppedRes == ZkResult.SessionExpired)
                                return FollowerExitReason.SessionExpired;

                            return FollowerExitReason.UnexpectedFailure;
                        }

                        lastStopVersion = status.Version;
                    }
                    else if (status.CoordinatorStatus == CoordinatorStatus.ResourcesGranted)
                    {
                        if (lastStopVersion > 0)
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
                            if (startedRes != ZkResult.Ok && startedRes != ZkResult.NoZnode)
                            {
                                if (watchStatusRes.Result == ZkResult.SessionExpired)
                                    return FollowerExitReason.SessionExpired;

                                return FollowerExitReason.UnexpectedFailure;
                            }
                        }
                        else
                        {
                            this.logger.Info("Ignoring ResourcesGranted status as did not receive a StopActivity notification. Likely I am a new follower.");
                        }

                        lastStartVersion = status.Version;
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

            if (this.followerToken.IsCancellationRequested)
            {
                await this.zooKeeperService.CloseSessionAsync();
                return FollowerExitReason.Cancelled;
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