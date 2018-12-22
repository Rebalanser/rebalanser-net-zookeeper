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
using RB = Rebalanser.ZooKeeper.ResourceBarrier;

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
        private TimeSpan connectTimeout;
        private int clientNumber;
        private string clientId;
        private string clientPath;
        private Task mainTask;
        
        private static object startLockObj = new object();
        private bool started;
        private int epoch;
       
        public ZooKeeperProvider(string zookeeperHosts,
            string zookeeperRootPath,
            TimeSpan sessionTimeout,
            TimeSpan connectTimeout,
            RebalancingMode rebalancingMode,
            ILogger logger,
            IZooKeeperService zooKeeperService=null)
        {
            this.zooKeeperRootPath = zookeeperRootPath;
            this.rebalancingMode = rebalancingMode;
            this.sessionTimeout = sessionTimeout;
            this.connectTimeout = connectTimeout;
            this.logger = logger;
            this.store = new ResourceGroupStore();
            this.clientPath = "";
            this.clientId = "Pending Id";

            if (zooKeeperService == null)
                this.zooKeeperService = new ZooKeeperService(zookeeperHosts, logger);
            else
                this.zooKeeperService = zooKeeperService;
        }

        public async Task StartAsync(string resourceGroup, 
            OnChangeActions onChangeActions, 
            CancellationToken token, 
            ClientOptions clientOptions)
        {
            // just in case someone does some concurrency
            lock (startLockObj)
            {
                if (this.started)
                    throw new RebalanserException("Client already started");

                this.started = true;
            }

            this.resourceGroup = resourceGroup;
            
            mainTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    var sessionTerm = await StartSessionAsync(onChangeActions, token, clientOptions);
                    this.logger.Info("-", $"Session terminated due to SessionTermination termination reason {sessionTerm}");

                    if (sessionTerm == ProcessStatus.Cancelled)
                    {
                        InvokeOnStop(onChangeActions);
                        break;
                    }

                    if (sessionTerm == ProcessStatus.ExpiredSession || sessionTerm == ProcessStatus.CouldNotEstablishSession)
                    {
                        if (clientOptions.AutoRecoveryOnError)
                        {
                            this.logger.Info(this.clientId, $"A new session will be created in {clientOptions.RestartDelay}");
                            await WaitFor(clientOptions.RestartDelay, token);
                            continue;
                        }
                        
                        var message = "Lost or could not establish a ZooKeeper session. Auto-recovery is not configured.";
                        NotifyOfError(onChangeActions, message, clientOptions.AutoRecoveryOnError, null);
                        this.logger.Info(this.clientId, message);
                        break;
                    }

                    if (sessionTerm == ProcessStatus.NonRecoverableError)
                    {
                        NotifyOfError(onChangeActions, "An unrecoverable error has occurred,that auto-recovery cannot handle. Check the log for details", clientOptions.AutoRecoveryOnError, null);
                        break;
                    }
                }
                
                this.logger.Info(this.clientId, "Rebalanser client terminated");
                this.started = false;
            });

            await Task.Yield();
        }

        private async Task<ProcessStatus> StartSessionAsync(OnChangeActions onChangeActions, 
            CancellationToken token, 
            ClientOptions clientOptions)
        {
            var processStatus = await RegisterClientZnode();
            if (processStatus != ProcessStatus.Valid)
                return processStatus;

            while (!token.IsCancellationRequested && this.zooKeeperService.GetKeeperState() != Watcher.Event.KeeperState.Expired)
            {
                this.logger.Info(this.clientId, $"Entering current session client");
                var connected = await BlockTillConnected(token);
                if (!connected)
                {
                    if(this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                        return ProcessStatus.ExpiredSession;
                    
                    return ProcessStatus.CouldNotEstablishSession;
                }

                var epochRes = await this.zooKeeperService.GetEpochAsync();
                if (epochRes.Result != ZkResult.Ok)
                {
                    switch (epochRes.Result)
                    {
                        case ZkResult.ConnectionLost:
                        case ZkResult.SessionExpired:
                            return ProcessStatus.ExpiredSession;
                        default:
                            return ProcessStatus.NonRecoverableError;
                    }
                }

                this.epoch = epochRes.Data;

                this.logger.Info(this.clientId, $"Finding next smaller sibling to watch");
                var siblingPath = await GetSiblingToWatchAsync();
                if (this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                    return ProcessStatus.ExpiredSession;

                if (siblingPath == string.Empty)
                {
                    // this method exits when its status as coordinator has ended due to cancellation, session expiry,
                    // error or detecting loss of leadership
                    processStatus = await RunAsCoordinatorAsync(onChangeActions, token);
                    switch (processStatus)
                    {
                        case ProcessStatus.Cancelled:
                        case ProcessStatus.ExpiredSession:
                        case ProcessStatus.NonRecoverableError:
                        case ProcessStatus.CouldNotEstablishSession:
                            return processStatus;
                        case ProcessStatus.RoleChange:
                        case ProcessStatus.Valid:
                            continue;
                        case ProcessStatus.ErrorWithValidSession:
                            break;
                    }
                }
                else
                {
                    // this method exits when its status as follower has ended due to cancellation, session expiry,
                    // error or detecting it should be the coordinator
                    processStatus = await RunAsFollowerAsync(onChangeActions, token, siblingPath);
                    switch (processStatus)
                    {
                        case ProcessStatus.Cancelled:
                        case ProcessStatus.ExpiredSession:
                        case ProcessStatus.NonRecoverableError:
                        case ProcessStatus.CouldNotEstablishSession:
                            return processStatus;
                        case ProcessStatus.RoleChange:
                            continue;
                        case ProcessStatus.ErrorWithValidSession:
                        case ProcessStatus.Valid:
                            break;
                    }
                }
                

                // if we got here then neither cancellation nor some kind of session related error has occurred
                // if we have auto-recovery enabled then we wait then try again with the existing session
                // else we'll exit with a non-recoverable error code
                if (clientOptions.AutoRecoveryOnError)
                {
                    this.logger.Info(this.clientId, $"Had to exit current role (coordinator or follower) and auto-recovery in progress. The session is still valid.");
                    await WaitFor(clientOptions.RestartDelay, token);
                }
                else
                {
                    this.logger.Info(this.clientId, $"Had to exit current role (coordinator or follower) and auto-recovery is disabled. Exiting the current session context.");
                    return ProcessStatus.NonRecoverableError;
                }
            }

            if(this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                return ProcessStatus.ExpiredSession;
            
            if (token.IsCancellationRequested)
                return ProcessStatus.Cancelled;
            
            return ProcessStatus.NonRecoverableError;
        }

        private async Task<ProcessStatus> RegisterClientZnode()
        {
            this.logger.Info(this.clientId, "Opening new session");
            // blocks until the session starts
            var connnnected = await this.zooKeeperService.StartSessionAsync(this.sessionTimeout, this.connectTimeout);
            if (!connnnected)
                return ProcessStatus.CouldNotEstablishSession;
            
            this.logger.Info(this.clientId, "Initializing zookeeper client paths");

            var initialized = false;
            switch (this.rebalancingMode)
            {
                case RebalancingMode.GlobalBarrier:
                    initialized = await this.zooKeeperService.InitializeGlobalBarrierAsync(
                        $"{this.zooKeeperRootPath}/{this.resourceGroup}/clients",
                        $"{this.zooKeeperRootPath}/{this.resourceGroup}/status",
                        $"{this.zooKeeperRootPath}/{this.resourceGroup}/stopped",
                        $"{this.zooKeeperRootPath}/{this.resourceGroup}/resources",
                        $"{this.zooKeeperRootPath}/{this.resourceGroup}/epoch");
                    break;
                case RebalancingMode.ResourceBarrier:
                    initialized = await this.zooKeeperService.InitializeResourceBarrierAsync(
                        $"{this.zooKeeperRootPath}/{this.resourceGroup}/clients",
                        $"{this.zooKeeperRootPath}/{this.resourceGroup}/resources",
                        $"{this.zooKeeperRootPath}/{this.resourceGroup}/epoch");
                    break;
                default:
                    initialized = false;
                    break;
            }

            if (!initialized)
            {
                var msg =
                    "Could not start a new rebalanser client due to failure to initialize the ZooKeeper client.";
                this.logger.Error(this.clientId, msg);
                return ProcessStatus.NonRecoverableError;
            }
            
            var createRes = await this.zooKeeperService.CreateClientAsync();
            if (createRes.Result == ZkResult.SessionExpired)
                return ProcessStatus.ExpiredSession;
            if (createRes.Result == ZkResult.NoZnode || createRes.Result == ZkResult.ConnectionLost)
                return ProcessStatus.CouldNotEstablishSession;
            
            this.clientPath = createRes.Data;
            this.SetIdFromPath();
            this.logger.Info(this.clientId, $"Client znode registered with Id {this.clientId}");

            return ProcessStatus.Valid;
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

        private async Task<ProcessStatus> RunAsCoordinatorAsync(OnChangeActions onChangeActions, 
            CancellationToken token)
        {
            this.logger.Info(this.clientId, $"As client {this.clientId}, I am the smallest sibling and therefore the leader");
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
                    coordinator = new RB.Coordinator(this.zooKeeperService,
                        this.logger,
                        this.store,
                        onChangeActions,
                        this.clientId,
                        token);
                    break;
                default:
                    throw new Exception(); // TODO
            }
            
            var hasBecome = await coordinator.BecomeCoordinatorAsync(this.epoch);
            switch (hasBecome)
            {
                case BecomeCoordinatorResult.Ok:
                    this.logger.Info(this.clientId, $"Have become the coordinator");
                    // this blocks until coordinator terminates (due to failure, session expiry or detects it is a zombie)
                    var coordinatorExitReason = await coordinator.StartEventLoopAsync();
                    this.logger.Info(this.clientId, $"The coordinator has exited for reason {coordinatorExitReason}");
                    if (coordinatorExitReason == CoordinatorExitReason.Cancelled)
                        return ProcessStatus.Cancelled;
                    if (coordinatorExitReason == CoordinatorExitReason.SessionExpired)
                        return ProcessStatus.ExpiredSession;

                    break;
                case BecomeCoordinatorResult.StaleEpoch:
                    this.logger.Info(this.clientId, 
                        "Since being elected, the epoch has been incremented suggesting another leader. Aborting coordinator role to check leadership again");
                    return ProcessStatus.RoleChange;
                default:
                    this.logger.Error(this.clientId, "Could not become coordinator");
                    break;
            }

            if (this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                return ProcessStatus.ExpiredSession;
            
            return ProcessStatus.ErrorWithValidSession;
        }

        private async Task<ProcessStatus> RunAsFollowerAsync(OnChangeActions onChangeActions, 
            CancellationToken token,
            string siblingPath)
        {
            this.logger.Info(this.clientId, $"I am not the smallest sibling, becoming a follower");

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
                    follower = new RB.Follower(this.zooKeeperService,
                        this.logger,
                        this.store,
                        onChangeActions,
                        this.clientId,
                        this.clientNumber,
                        siblingPath,
                        token);
                    break;
                default:
                    throw new Exception(); // TODO
            }

            var followerInitialized = await follower.BecomeFollowerAsync();
            if (followerInitialized)
            {
                this.logger.Info(this.clientId, $"Have become a follower, starting follower event loop");
                // blocks until follower either fails, the session expires or the follower detects it might be the new leader
                var followerExitReason = await follower.StartEventLoopAsync();
                this.logger.Info(this.clientId, $"The follower has exited for reason {followerExitReason}");
                switch (followerExitReason)
                {
                    case FollowerStatus.IsNewLeader: return ProcessStatus.RoleChange;
                    case FollowerStatus.SessionExpired: return ProcessStatus.ExpiredSession; 
                    case FollowerStatus.Cancelled: return ProcessStatus.Cancelled;
                }
            }
            else
            {
                this.logger.Error(this.clientId, "Could not become a follower");
                if(this.zooKeeperService.GetKeeperState() == Watcher.Event.KeeperState.Expired)
                    return ProcessStatus.ExpiredSession;
            }

            return ProcessStatus.ErrorWithValidSession;
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
            var maxClientNumber = -1;
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

                if (maxClientNumber == -1)
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
                this.logger.Error(this.clientId, ex.ToString());
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
                this.logger.Error(this.clientId, ex.ToString());
            }
        }
    }
}