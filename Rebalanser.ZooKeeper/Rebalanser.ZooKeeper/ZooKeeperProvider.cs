using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.Store;
using Rebalanser.ZooKeeper.Zk;
using GB = Rebalanser.ZooKeeper.GlobalBarrier;

namespace Rebalanser.ZooKeeper
{
    public class ZooKeeperProvider : IRebalanserProvider
    {
        private string zooKeeperRootPath;
        private string resourceGroup;
        private ILogger logger;
        private IZooKeeperService zooKeeperService;
        private ResourceGroupStore store;
        private RebalancingMode rebalancingMode;
        private TimeSpan sessionTimeout;
        private int clientNumber;
        private string clientId;
        private string clientPath;
        private Task mainTask;
        
        private static object startLockObj = new object();
        private bool started;
       
        public ZooKeeperProvider(string zookeeperHosts,
            string zookeeperRootPath,
            TimeSpan sessionTimeout,
            RebalancingMode rebalancingMode,
            ILogger logger,
            IZooKeeperService zooKeeperService=null)
        {
            this.zooKeeperRootPath = zookeeperRootPath;
            this.rebalancingMode = rebalancingMode;
            this.sessionTimeout = sessionTimeout;
            this.logger = logger;
            this.store = new ResourceGroupStore();
            this.clientPath = "";
            this.clientId = "";

            if (zooKeeperService == null)
                this.zooKeeperService = new ZooKeeperService(zookeeperHosts, logger);
            else
                this.zooKeeperService = zooKeeperService;
        }

        public async Task StartAsync(string resourceGroup, OnChangeActions onChangeActions, CancellationToken token, ContextOptions contextOptions)
        {
            // just in case someone does some concurrency
            lock (startLockObj)
            {
                if (this.started)
                    throw new RebalanserException("Context already started");

                this.started = true;
            }

            this.resourceGroup = resourceGroup;
            
            mainTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    var sessionTerm = await StartSessionAsync(onChangeActions, token, contextOptions);
                    this.logger.Info($"Session terminated due to SessionTermination termination reason {sessionTerm}");
                    if (sessionTerm == SessionTermination.Cancelled)
                        break;

                    if (sessionTerm == SessionTermination.Expired || sessionTerm == SessionTermination.CouldNotEstablishSession)
                    {
                        this.logger.Info("A new session will be created in 5 seconds");
                        await WaitFor(TimeSpan.FromSeconds(5), token);
                        continue;
                    }

                    if (sessionTerm == SessionTermination.NonRecoverableError)
                    {
                        NotifyOfError(onChangeActions, "An unrecoverable error has occurred,that auto-recovery cannot handle. Check the log for details", contextOptions.AutoRecoveryOnError, null);
                        break;
                    }
                }
                
                this.logger.Info("Rebalanser context terminated");
                this.started = false;
            });

            await Task.Yield();
        }

        private async Task<SessionTermination> StartSessionAsync(OnChangeActions onChangeActions, 
            CancellationToken token, 
            ContextOptions contextOptions)
        {
            this.logger.Info("Opening new session");
            // blocks until the session starts
            await this.zooKeeperService.StartSessionAsync(this.sessionTimeout);
            
            this.logger.Info("Initializing zookeeper client paths");
            var initialized = await this.zooKeeperService.InitializeAsync(
                $"{this.zooKeeperRootPath}/{this.resourceGroup}/clients",
                $"{this.zooKeeperRootPath}/{this.resourceGroup}/status",
                $"{this.zooKeeperRootPath}/{this.resourceGroup}/stopped",
                $"{this.zooKeeperRootPath}/{this.resourceGroup}/resources",
                $"{this.zooKeeperRootPath}/{this.resourceGroup}/epoch");

            if (!initialized)
            {
                var msg =
                    "Could not start a new rebalanser context due to failure to initialize the ZooKeeper client.";
                this.logger.Error(msg);
                return SessionTermination.NonRecoverableError;
            }
            
            var createRes = await this.zooKeeperService.CreateClientAsync();
            if (createRes.Result == ZkResult.SessionExpired)
                return SessionTermination.Expired;
            if (createRes.Result == ZkResult.NoZnode || createRes.Result == ZkResult.ConnectionLost)
                return SessionTermination.CouldNotEstablishSession;
            
            this.clientPath = createRes.Data;
            this.SetIdFromPath();
            this.logger.Info($"Client znode registered with Id {this.clientId}");

            while (!token.IsCancellationRequested && this.zooKeeperService.GetKeeperState() != Watcher.Event.KeeperState.Expired)
            {
                this.logger.Info($"Entering current session context");
                var connected = await BlockTillConnected(token);
                if (!connected)
                    break;
                
                this.logger.Info($"Finding next smaller sibling to watch");
                var siblingPath = await GetSiblingToWatchAsync();
                if (this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                    return SessionTermination.Expired;

                if (siblingPath == string.Empty)
                {
                    this.logger.Info($"I am the smallest sibling and therefore the leader");
                    ICoordinator coordinator;
                    switch (this.rebalancingMode)
                    {
                        case RebalancingMode.GlobalBarrier:
                            coordinator = new GB.Coordinator(this.zooKeeperService,
                                this.logger,
                                this.store,
                                onChangeActions,
                                this.clientId,
                                token);
                            break;
                        case RebalancingMode.ResourceBarrier:
                            throw new Exception(); // TODO
                        default:
                            throw new Exception(); // TODO
                    }
                    
                    var hasBecome = await coordinator.BecomeCoordinatorAsync();
                    if (hasBecome)
                    {
                        this.logger.Info($"Have become the coordinator");
                        // this blocks until coordinator terminates (due to failure, session expiry or detects it is a zombie)
                        var coordinatorExitReason = await coordinator.StartEventLoopAsync(); 
                        this.logger.Info($"The coordinator has exited for reason {coordinatorExitReason}");
                        if (coordinatorExitReason == CoordinatorExitReason.Cancelled)
                            return SessionTermination.Cancelled;
                        if (coordinatorExitReason == CoordinatorExitReason.SessionExpired)
                            return SessionTermination.Expired;
                    }
                    else
                    {
                        this.logger.Error("Could not become coordinator");
                        if (this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                            return SessionTermination.Expired;
                    }
                }
                else
                {
                    this.logger.Info($"I am not the smallest sibling, becoming a follower");

                    IFollower follower;
                    switch (this.rebalancingMode)
                    {
                        case RebalancingMode.GlobalBarrier:
                            follower = new GB.Follower(this.zooKeeperService,
                                this.logger,
                                this.store,
                                onChangeActions,
                                this.clientId,
                                this.clientNumber,
                                siblingPath,
                                token);
                            break;
                        case RebalancingMode.ResourceBarrier:
                            throw new Exception(); // TODO
                        default:
                            throw new Exception(); // TODO
                    }

                    var followerInitialized = await follower.BecomeFollowerAsync();
                    if (followerInitialized)
                    {
                        this.logger.Info($"Have become a follower, starting follower event loop");
                        // blocks until follower either fails, the session expires or the follower detects it might be the new leader
                        var followerExitReason = await follower.StartEventLoopAsync();
                        this.logger.Info($"The follower has exited for reason {followerExitReason}");
                        switch (followerExitReason)
                        {
                            case FollowerExitReason.IsNewLeader: continue;
                            case FollowerExitReason.SessionExpired: return SessionTermination.Expired; 
                            case FollowerExitReason.Cancelled: return SessionTermination.Cancelled;
                        }
                    }
                    else
                    {
                        this.logger.Error("Could not become a follower");
                        if(this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                            return SessionTermination.Expired;
                    }
                }

                // if we got here then neither cancellation nor some kind of session related error has occurred
                // if we have auto-recovery enabled then we wait then try again with the existing session
                // else we'll exit with a non-recoverable error code
                if (contextOptions.AutoRecoveryOnError)
                {
                    this.logger.Info($"A failure occurred and auto-recovery in progress. The session is still valid.");
                    await WaitFor(contextOptions.RestartDelay, token);
                }
                else
                {
                    this.logger.Info($"A failure occurred and auto-recovery is disabled. Exiting the current session context.");
                    return SessionTermination.NonRecoverableError;
                }
            }

            if(this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                return SessionTermination.Expired;
            
            if (token.IsCancellationRequested)
                return SessionTermination.Cancelled;
            
            return SessionTermination.NonRecoverableError;
        }

        private async Task<bool> BlockTillConnected(CancellationToken token)
        {
            while (!token.IsCancellationRequested && this.zooKeeperService.GetKeeperState() != Watcher.Event.KeeperState.SyncConnected)
            {
                if (this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                    return false;

                await WaitFor(TimeSpan.FromSeconds(1), token);
            }

            return true;
        }

        private async Task WaitFor(TimeSpan waitPeriod, CancellationToken token)
        {
            try
            {
                await Task.Delay(waitPeriod, token);
            }
            catch (TaskCanceledException)
            {}
        }

        public async Task WaitForCompletionAsync()
        {
            await this.mainTask;
        }

        public IList<string> GetAssignedResources()
        {
            while (true)
            {
                var response = this.store.GetResources();
                if (response.AssignmentStatus == AssignmentStatus.ResourcesAssigned || response.AssignmentStatus == AssignmentStatus.NoResourcesAssigned)
                    return response.Resources;
                
                Thread.Sleep(100);
            }
        }

        public IList<string> GetAssignedResources(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var response = this.store.GetResources();
                if (response.AssignmentStatus == AssignmentStatus.ResourcesAssigned || response.AssignmentStatus == AssignmentStatus.NoResourcesAssigned)
                    return response.Resources;

                Thread.Sleep(100);
            }

            return new List<string>();
        }

        private void SetIdFromPath()
        {
            this.clientNumber = int.Parse(this.clientPath.Substring(this.clientPath.Length - 10, 10));
            this.clientId = this.clientPath.Substring(this.clientPath.LastIndexOf("/", StringComparison.Ordinal)+1);
        }

        private async Task<string> GetSiblingToWatchAsync()
        {
            var maxClientNumber = 0;
            var watchChild = string.Empty;
            var clientsRes = await this.zooKeeperService.GetActiveClientsAsync();
            if (clientsRes.Result == ZkResult.Ok)
            {
                var clients = clientsRes.Data;
                foreach (var childPath in clients.ClientPaths)
                {
                    var siblingClientNumber = int.Parse(childPath.Substring(childPath.Length - 10, 10));
                    if (siblingClientNumber > maxClientNumber && siblingClientNumber < this.clientNumber)
                    {
                        watchChild = childPath;
                        maxClientNumber = siblingClientNumber;
                    }
                }

                if (maxClientNumber == 0)
                    return string.Empty;
                
                return watchChild;
            }

            return string.Empty;
        }
        
        private async Task NotifyOfErrorAsync(Task faultedTask, string message, bool autoRecoveryEnabled, OnChangeActions onChangeActions)
        {
            await InvokeOnErrorAsync(faultedTask, message, autoRecoveryEnabled, onChangeActions);
            InvokeOnStop(onChangeActions);
        }

        private void NotifyOfError(OnChangeActions onChangeActions, string message, bool autoRecoveryEnabled, Exception exception)
        {
            InvokeOnError(onChangeActions, message, autoRecoveryEnabled, exception);
            InvokeOnStop(onChangeActions);
        }

        private async Task InvokeOnErrorAsync(Task faultedTask, string message, bool autoRecoveryEnabled, OnChangeActions onChangeActions)
        {
            try
            {
                await faultedTask;
            }
            catch (Exception ex)
            {
                InvokeOnError(onChangeActions, message, autoRecoveryEnabled, ex);
            }
        }

        private void InvokeOnError(OnChangeActions onChangeActions, string message, bool autoRecoveryEnabled, Exception exception)
        {
            try
            {
                foreach (var onErrorAction in onChangeActions.OnErrorActions)
                    onErrorAction.Invoke(message, autoRecoveryEnabled, exception);
            }
            catch (Exception ex)
            {
                this.logger.Error(ex.ToString());
            }
        }

        private void InvokeOnStop(OnChangeActions onChangeActions)
        {
            try
            {
                foreach (var onErrorAction in onChangeActions.OnStopActions)
                    onErrorAction.Invoke();
            }
            catch (Exception ex)
            {
                this.logger.Error(ex.ToString());
            }
        }
    }
}