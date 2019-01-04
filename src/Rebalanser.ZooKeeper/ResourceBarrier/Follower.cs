using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.GlobalBarrier;
using Rebalanser.ZooKeeper.ResourceManagement;
using Rebalanser.ZooKeeper.Zk;

namespace Rebalanser.ZooKeeper.ResourceBarrier
{
    public class Follower : Watcher, IFollower
    {
        // services
        private IZooKeeperService zooKeeperService;
        private ILogger logger;
        private ResourceManager store;
        
        // immutable state
        private string clientId;
        private int clientNumber;
        private CancellationToken followerToken;
        
        // mutable state
        private string watchSiblingPath;
        private string siblingId;
        private Task rebalancingTask;
        private CancellationTokenSource rebalancingCts;
        private BlockingCollection<FollowerEvent> events;
        private bool ignoreWatches;
        
        public Follower(IZooKeeperService zooKeeperService,
            ILogger logger,
            ResourceManager store,
            string clientId,
            int clientNumber,
            string watchSiblingPath,
            CancellationToken followerToken)
        {
            this.zooKeeperService = zooKeeperService;
            this.logger = logger;
            this.store = store;
            this.clientId = clientId;
            this.clientNumber = clientNumber;
            this.watchSiblingPath = watchSiblingPath;
            this.siblingId = watchSiblingPath.Substring(watchSiblingPath.LastIndexOf("/", StringComparison.Ordinal));
            this.followerToken = followerToken;
            
            this.rebalancingCts = new CancellationTokenSource();
            this.events = new BlockingCollection<FollowerEvent>();
        }

        public async Task<BecomeFollowerResult> BecomeFollowerAsync()
        {
            try
            {
                this.ignoreWatches = false;
                await this.zooKeeperService.WatchSiblingNodeAsync(this.watchSiblingPath, this);
                this.logger.Info(this.clientId, $"Follower - Set a watch on sibling node {this.watchSiblingPath}");

                await this.zooKeeperService.WatchResourcesDataAsync(this);
                this.logger.Info(this.clientId, $"Follower - Set a watch on resources node");
            }
            catch (ZkNoEphemeralNodeWatchException)
            {
                this.logger.Info(this.clientId, "Follower - Could not set a watch on the sibling node as it has gone");
                return BecomeFollowerResult.WatchSiblingGone;
            }
            catch (Exception e)
            {
                this.logger.Error("Follower - Could not become a follower due to an error", e);
                return BecomeFollowerResult.Error;
            }

            return BecomeFollowerResult.Ok;
        }
        
        
        // Important that nothing throws an exception in this method as it is called from the zookeeper library
        public override async Task process(WatchedEvent @event)
        {
            if (this.followerToken.IsCancellationRequested || this.ignoreWatches)
                return;
                
            if(@event.getPath() != null)
                this.logger.Info(this.clientId, $"Follower - KEEPER EVENT {@event.getState()} - {@event.get_Type()} - {@event.getPath()}");
            else 
                this.logger.Info(this.clientId, $"Follower - KEEPER EVENT {@event.getState()} - {@event.get_Type()}");
            
            switch (@event.getState())
            {
                case Event.KeeperState.Expired:
                    this.events.Add(FollowerEvent.SessionExpired);
                    break;
                case Event.KeeperState.Disconnected:
                    break;
                case Event.KeeperState.ConnectedReadOnly:
                case Event.KeeperState.SyncConnected:
                    if (@event.get_Type() == Event.EventType.NodeDeleted)
                    {
                        if (@event.getPath().EndsWith(this.siblingId))
                        {
                            await PerformLeaderCheckAsync();
                        }
                        else
                        {
                            this.logger.Error(this.clientId, $"Follower - Unexpected node deletion detected of {@event.getPath()}");
                            this.events.Add(FollowerEvent.PotentialInconsistentState);
                        }
                    }
                    else if (@event.get_Type() == Event.EventType.NodeDataChanged)
                    {
                        if (@event.getPath().EndsWith("resources"))
                            await SendTriggerRebalancingEvent();
                    }

                    break;
                default:
                    this.logger.Error(this.clientId,
                        $"Follower - Currently this library does not support ZooKeeper state {@event.getState()}");
                    this.events.Add(FollowerEvent.PotentialInconsistentState);
                    break;
            }
        }
        
        public async Task<FollowerExitReason> StartEventLoopAsync()
        {
            // it is possible that rebalancing has been triggered already, so check 
            // if any resources have been assigned already and if so, add a RebalancingTriggered event
            await CheckForRebalancingAsync();
            
            while (!this.followerToken.IsCancellationRequested)
            {
                FollowerEvent followerEvent;
                if (this.events.TryTake(out followerEvent))
                {
                    switch (followerEvent)
                    {
                        case FollowerEvent.SessionExpired:
                            await CleanUpAsync();
                            return FollowerExitReason.SessionExpired;

                        case FollowerEvent.IsNewLeader:
                            await CleanUpAsync();
                            return FollowerExitReason.PossibleRoleChange;

                        case FollowerEvent.PotentialInconsistentState:
                            await CleanUpAsync();
                            return FollowerExitReason.PotentialInconsistentState;
                        
                        case FollowerEvent.FatalError:
                            await CleanUpAsync();
                            return FollowerExitReason.FatalError;

                        case FollowerEvent.RebalancingTriggered:
                            if (this.events.Any())
                            {
                                // skip this event. All other events take precedence over rebalancing
                                // there may be multiple rebalancing events, so if the events collection
                                // consists only of rebalancing events then we'll just process the last one
                            }
                            else
                            {
                                await CancelRebalancingIfInProgressAsync();
                                logger.Info(this.clientId, "Follower - Rebalancing triggered");
                                rebalancingTask = Task.Run(async () =>
                                    await RespondToRebalancing(this.rebalancingCts.Token));
                            }

                            break;
                        
                        default:
                            await CleanUpAsync();
                            return FollowerExitReason.PotentialInconsistentState;
                    }
                }

                await WaitFor(1000);
            }

            if (this.followerToken.IsCancellationRequested)
            {
                await CleanUpAsync();
                await this.zooKeeperService.CloseSessionAsync();
                return FollowerExitReason.Cancelled;
            }

            return FollowerExitReason.PotentialInconsistentState;
        }

        private async Task SendTriggerRebalancingEvent()
        {
            try
            {
                await this.zooKeeperService.WatchResourcesDataAsync(this);
                this.events.Add(FollowerEvent.RebalancingTriggered);
            }
            catch (Exception e)
            {
                this.logger.Error("Could not put a watch on the resources node", e);
                this.events.Add(FollowerEvent.PotentialInconsistentState);
            }
        }

        private async Task CheckForRebalancingAsync()
        {
            var resources = await this.zooKeeperService.GetResourcesAsync(null, null);
            var assignedResources = resources.ResourceAssignments.Assignments
                .Where(x => x.ClientId.Equals(this.clientId))
                .Select(x => x.Resource)
                .ToList();
            
            if(assignedResources.Any())
                this.events.Add(FollowerEvent.RebalancingTriggered);
        }
        
        private async Task RespondToRebalancing(CancellationToken rebalancingToken)
        {
            try
            {
                var result = await ProcessStatusChangeAsync(rebalancingToken);
                switch (result)
                {
                    case RebalancingResult.Complete:
                        logger.Info(this.clientId, "Follower - Rebalancing complete");
                        break;

                    case RebalancingResult.Cancelled:
                        logger.Info(this.clientId, "Follower - Rebalancing cancelled");
                        break;

                    default:
                        this.logger.Error(this.clientId,
                            $"Follower - A non-supported RebalancingResult has been returned: {result}");
                        this.events.Add(FollowerEvent.PotentialInconsistentState);
                        break;
                }
            }
            catch (ZkSessionExpiredException)
            {
                this.logger.Warn(this.clientId, $"Follower - The session was lost during rebalancing");
                this.events.Add(FollowerEvent.SessionExpired);
            }
            catch (ZkOperationCancelledException)
            {
                this.logger.Warn(this.clientId, $"Follower - The rebalancing has been cancelled");
            }
            catch (InconsistentStateException e)
            {
                this.logger.Error(this.clientId, $"Follower - An error occurred potentially leaving the client in an inconsistent state. Termination of the client or creationg of a new session will follow", e);
                this.events.Add(FollowerEvent.PotentialInconsistentState);
            }
            catch (TerminateClientException e)
            {
                this.logger.Error(this.clientId, $"Follower - A fatal error occurred, aborting", e);
                this.events.Add(FollowerEvent.FatalError);
            }
            catch (Exception e)
            {
                this.logger.Error(this.clientId, $"Follower - Rebalancing failed.", e);
                this.events.Add(FollowerEvent.PotentialInconsistentState);
            }
        }

        private async Task<RebalancingResult> ProcessStatusChangeAsync(CancellationToken rebalancingToken)
        {
            await this.store.InvokeOnStopActionsAsync(this.clientId, "Follower");
            
            var resources = await this.zooKeeperService.GetResourcesAsync(null, null);
            var assignedResources = resources.ResourceAssignments.Assignments
                .Where(x => x.ClientId.Equals(this.clientId))
                .Select(x => x.Resource)
                .ToList();

            await this.store.InvokeOnStartActionsAsync(this.clientId, "Follower", assignedResources, rebalancingToken, this.followerToken);
            
            return RebalancingResult.Complete;
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
                await this.store.InvokeOnStopActionsAsync(this.clientId, "Follower");
            }
        }
        
        private async Task CancelRebalancingIfInProgressAsync()
        {
            if (this.rebalancingTask != null && !this.rebalancingTask.IsCompleted)
            {
                logger.Info(this.clientId, "Follower - Cancelling the rebalancing that is in progress");
                this.rebalancingCts.Cancel();
                try
                {
                    await this.rebalancingTask; // might need to put a time limit on this
                }
                catch (Exception ex)
                {
                    this.logger.Error(this.clientId, "Follower - Errored on cancelling rebalancing", ex);
                    this.events.Add(FollowerEvent.PotentialInconsistentState);
                }
                this.rebalancingCts = new CancellationTokenSource(); // reset cts
            }
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
        
        private async Task PerformLeaderCheckAsync()
        {
            bool checkComplete = false;
            while (!checkComplete)
            {
                try
                {
                    int maxClientNumber = -1;
                    string watchChild = string.Empty;
                    var clients = await this.zooKeeperService.GetActiveClientsAsync();

                    foreach (var childPath in clients.ClientPaths)
                    {
                        int siblingClientNumber = int.Parse(childPath.Substring(childPath.Length - 10, 10));
                        if (siblingClientNumber > maxClientNumber && siblingClientNumber < this.clientNumber)
                        {
                            watchChild = childPath;
                            maxClientNumber = siblingClientNumber;
                        }
                    }

                    if (maxClientNumber == -1)
                    {
                        this.events.Add(FollowerEvent.IsNewLeader);
                    }
                    else
                    {
                        this.watchSiblingPath = watchChild;
                        this.siblingId = watchSiblingPath.Substring(watchChild.LastIndexOf("/", StringComparison.Ordinal));
                        await this.zooKeeperService.WatchSiblingNodeAsync(watchChild, this);
                        this.logger.Info(this.clientId, $"Follower - Set a watch on sibling node {this.watchSiblingPath}");
                    }

                    checkComplete = true;
                }
                catch (ZkNoEphemeralNodeWatchException)
                {
                    // do nothing except wait, the next iteration will find
                    // another client or it wil detect that it itself is the new leader
                    await WaitFor(1000);
                }
                catch (ZkSessionExpiredException)
                {
                    this.events.Add(FollowerEvent.SessionExpired);
                    checkComplete = true;
                }
                catch (Exception ex)
                {
                    this.logger.Error(this.clientId, "Follower - Failed looking for sibling to watch", ex);
                    this.events.Add(FollowerEvent.PotentialInconsistentState);
                    checkComplete = true;
                }
            }
        }
    }
}