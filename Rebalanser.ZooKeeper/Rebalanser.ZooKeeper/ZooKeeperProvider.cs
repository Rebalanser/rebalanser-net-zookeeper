using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.ResourceManagement;
using Rebalanser.ZooKeeper.Zk;
using GB = Rebalanser.ZooKeeper.GlobalBarrier;
using RB = Rebalanser.ZooKeeper.ResourceBarrier;

namespace Rebalanser.ZooKeeper
{
    public class ZooKeeperProvider : IRebalanserProvider
    {
        // services
        private ILogger logger;
        private IZooKeeperService zooKeeperService;
        private ResourceManager store;

        // non-mutable state
        private readonly RebalancingMode rebalancingMode;
        private readonly string zooKeeperRootPath;
        private readonly TimeSpan minimumRebalancingInterval;
        private string resourceGroup;
        private TimeSpan sessionTimeout;
        private TimeSpan connectTimeout;
        private object startLockObj = new object();
        private Random rand;
        
        // mutable state
        private string clientId;
        private string clientPath;
        private int clientNumber;
        private ClientStates state;
        private string watchSiblingNodePath;
        private int epoch;
        private bool started;
        private Task mainTask;

        public ZooKeeperProvider(string zookeeperHosts,
            string zooKeeperRootPath,
            TimeSpan sessionTimeout, 
            TimeSpan connectTimeout,
            TimeSpan minimumRebalancingInterval,
            RebalancingMode rebalancingMode,
            ILogger logger,
            IZooKeeperService zooKeeperService=null)
        {
            this.zooKeeperRootPath = zooKeeperRootPath;
            this.rebalancingMode = rebalancingMode;
            this.logger = logger;
            this.sessionTimeout = sessionTimeout;
            this.connectTimeout = connectTimeout;
            this.minimumRebalancingInterval = minimumRebalancingInterval;
            this.rand = new Random(Guid.NewGuid().GetHashCode());
            
            if (zooKeeperService == null)
                this.zooKeeperService = new ZooKeeperService(zookeeperHosts);
            else
                this.zooKeeperService = zooKeeperService;
        }

        public async Task StartAsync(string resourceGroup,
            OnChangeActions onChangeActions,
            CancellationToken token,
            ClientOptions clientOptions)
        {
            // just in case someone tries to start the client twice (with some concurrency)
            lock (startLockObj)
            {
                if (this.started)
                    throw new RebalanserException("Client already started");

                this.started = true;
            }
            
            this.store = new ResourceManager(this.zooKeeperService, this.logger, onChangeActions, clientOptions, this.rebalancingMode);
            this.resourceGroup = resourceGroup;
            SetStateToNoSession();

            this.resourceGroup = resourceGroup;

            mainTask = Task.Run(async () => await RunStateMachine(token, clientOptions));
            await Task.Yield();
        }

        private async Task RunStateMachine(CancellationToken token, ClientOptions clientOptions)
        {
            while (this.state != ClientStates.Terminated)
            {
                if (token.IsCancellationRequested)
                    await TerminateAsync("cancellation");
                
                try
                {
                    switch (this.state)
                    {
                        case ClientStates.NoSession:
                            var established = await EstablishSessionAsync(token);
                            switch (established)
                            {
                                case NewSessionResult.Established:
                                    this.state = ClientStates.NoClientNode;
                                    break;
                                case NewSessionResult.TimeOut:
                                    this.state = ClientStates.NoSession;
                                    await WaitRandomTime(TimeSpan.FromSeconds(5));
                                    break;
                                default:
                                    this.state = ClientStates.Error;
                                    break;
                            }

                            break;
                        case ClientStates.NoClientNode:
                            var created = await CreateClientNodeAsync();
                            if (created)
                                this.state = ClientStates.NoRole;
                            else
                                this.state = ClientStates.Error;
                            break;
                        case ClientStates.NoRole:
                            var epochAttained = await CacheEpochLocallyAsync();
                            if (!epochAttained)
                                await EvaluateTerminationAsync(token, clientOptions);

                            var (electionResult, lowerSiblingPath) = await DetermineLeadershipAsync();
                            switch (electionResult)
                            {
                                case ElectionResult.IsLeader:
                                    this.state = ClientStates.IsLeader;
                                    this.watchSiblingNodePath = string.Empty;
                                    break;
                                case ElectionResult.IsFollower:
                                    this.state = ClientStates.IsFollower;
                                    this.watchSiblingNodePath = lowerSiblingPath;
                                    break;
                                default:
                                    await EvaluateTerminationAsync(token, clientOptions);
                                    break;
                            }

                            break;
                        case ClientStates.IsLeader:
                            var coordinatorExitReason = await BecomeCoordinatorAsync(token);
                            switch (coordinatorExitReason)
                            {
                                case CoordinatorExitReason.NoLongerCoordinator:
                                    SetStateToNoSession(); // need a new client node
                                    break;
                                case CoordinatorExitReason.Cancelled:
                                    await TerminateAsync("cancellation");
                                    break;
                                case CoordinatorExitReason.SessionExpired:
                                    SetStateToNoSession();
                                    break;
                                case CoordinatorExitReason.PotentialInconsistentState:
                                    await EvaluateTerminationAsync(token, clientOptions);
                                    break;
                                case CoordinatorExitReason.FatalError:
                                    await TerminateAsync("fatal error");
                                    break;
                                default:
                                    await EvaluateTerminationAsync(token, clientOptions);
                                    break;
                            }

                            break;
                        case ClientStates.IsFollower:
                            var followerExitReason = await BecomeFollowerAsync(token);
                            switch (followerExitReason)
                            {
                                case FollowerExitReason.PossibleRoleChange:
                                    this.state = ClientStates.NoRole;
                                    break;
                                case FollowerExitReason.Cancelled:
                                    await TerminateAsync("cancellation");
                                    break;
                                case FollowerExitReason.SessionExpired:
                                    SetStateToNoSession();
                                    break;
                                case FollowerExitReason.FatalError:
                                    await TerminateAsync("fatal error");
                                    break;
                                case FollowerExitReason.PotentialInconsistentState:
                                    await EvaluateTerminationAsync(token, clientOptions);
                                    break;
                                default:
                                    await EvaluateTerminationAsync(token, clientOptions);
                                    break;
                            }

                            break;
                        case ClientStates.Error:
                            await EvaluateTerminationAsync(token, clientOptions);
                            break;
                        default:
                            await EvaluateTerminationAsync(token, clientOptions);
                            break;
                    }
                }
                catch (ZkSessionExpiredException)
                {
                    SetStateToNoSession();
                }
                catch (ZkOperationCancelledException)
                {
                    await TerminateAsync("cancellation");
                }
                catch (TerminateClientException e)
                {
                    this.logger.Error(this.clientId, "Fatal error", e);
                    await InvokeOnErrorActionsAsync("A fatal error has occurred, client terminated", e);
                    await TerminateAsync("Fatal error");
                }
                catch (InconsistentStateException e)
                {
                    this.logger.Error(this.clientId, "An error has caused that may have left the client in an inconsistent state.", e);
                    await InvokeOnErrorActionsAsync("Client error", e);
                    await EvaluateTerminationAsync(token, clientOptions);
                }
                catch (Exception e)
                {
                    this.logger.Error(this.clientId, "An unexpected error has been caught", e);
                    await InvokeOnErrorActionsAsync("Client error", e);
                    await EvaluateTerminationAsync(token, clientOptions);
                }
            }
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

        private void ResetMutableState()
        {
            this.clientId = string.Empty;
            this.clientNumber = -1;
            this.clientPath = string.Empty;
            this.watchSiblingNodePath = string.Empty;
            this.epoch = 0;
        }

        private async Task EvaluateTerminationAsync(CancellationToken token, ClientOptions clientOptions)
        {
            if (token.IsCancellationRequested)
            {
                await TerminateAsync("cancellation");
            }
            else if (clientOptions.AutoRecoveryOnError)
            {
                SetStateToNoSession();
                logger.Info(this.clientId, $"Unexpected error. Auto-recovery enabled. Will restart in {clientOptions.RestartDelay.TotalMilliseconds}ms.");
                await Task.Delay(clientOptions.RestartDelay);
            }
            else
            {
                await TerminateAsync("unexpected error has occurred with auto-recovery disabled");
            }
        }

        private void SetStateToNoSession()
        {
            ResetMutableState();
            this.state = ClientStates.NoSession;
        }

        private async Task<NewSessionResult> EstablishSessionAsync(CancellationToken token)
        {
            var randomWait = this.rand.Next(2000);
            this.logger.Info(this.clientId, $"Will try to open a new session in {randomWait}ms");
            await Task.Delay(randomWait);
            
            // blocks until the session starts or timesout
            var connected = await this.zooKeeperService.StartSessionAsync(this.sessionTimeout, this.connectTimeout, token);
            if (!connected)
            {
                this.logger.Error(this.clientId, "Failed to open a session, connect timeout exceeded");
                return NewSessionResult.TimeOut;
            }

            this.logger.Info(this.clientId, "Initializing zookeeper client paths");

            try
            {
                switch (this.rebalancingMode)
                {
                    case RebalancingMode.GlobalBarrier:
                        await this.zooKeeperService.InitializeGlobalBarrierAsync(
                            $"{this.zooKeeperRootPath}/{this.resourceGroup}/clients",
                            $"{this.zooKeeperRootPath}/{this.resourceGroup}/status",
                            $"{this.zooKeeperRootPath}/{this.resourceGroup}/stopped",
                            $"{this.zooKeeperRootPath}/{this.resourceGroup}/resources",
                            $"{this.zooKeeperRootPath}/{this.resourceGroup}/epoch");
                        break;
                    case RebalancingMode.ResourceBarrier:
                        await this.zooKeeperService.InitializeResourceBarrierAsync(
                            $"{this.zooKeeperRootPath}/{this.resourceGroup}/clients",
                            $"{this.zooKeeperRootPath}/{this.resourceGroup}/resources",
                            $"{this.zooKeeperRootPath}/{this.resourceGroup}/epoch");
                        break;
                }
            }
            catch (ZkInvalidOperationException e)
            {
                var msg =
                    "Could not start a new rebalanser client due to a problem with the prerequisite paths in ZooKeeper.";
                this.logger.Error(this.clientId, msg, e);
                return NewSessionResult.Error;
            }
            catch (Exception e)
            {
                var msg =
                    "An unexpected error occurred while intializing the rebalanser ZooKeeper paths";
                this.logger.Error(this.clientId, msg, e);
                return NewSessionResult.Error;
            }

            return NewSessionResult.Established;
        }
        
        private async Task<bool> CreateClientNodeAsync()
        {
            try
            {
                this.clientPath = await this.zooKeeperService.CreateClientAsync();
                this.SetIdFromPath();
                this.logger.Info(this.clientId, $"Client znode registered with Id {this.clientId}");
                return true;
            }
            catch (ZkInvalidOperationException e)
            {
                this.logger.Error(this.clientId, "Could not create the client znode.", e);
                return false;
            }
        }
        
        private void SetIdFromPath()
        {
            this.clientNumber = int.Parse(this.clientPath.Substring(this.clientPath.Length - 10, 10));
            this.clientId = this.clientPath.Substring(this.clientPath.LastIndexOf("/", StringComparison.Ordinal)+1);
        }

        private async Task<bool> CacheEpochLocallyAsync()
        {
            try
            {
                this.epoch = await this.zooKeeperService.GetEpochAsync();
                return true;
            }
            catch (ZkInvalidOperationException e)
            {
                this.logger.Error(this.clientId, "Could not cache the epoch. ", e);
                return false;
            }
        }
        
        private async Task<(ElectionResult, string)> DetermineLeadershipAsync()
        {
            this.logger.Info(this.clientId, $"Looking for a next smaller sibling to watch");
            var (success, lowerSiblingPath) = await FindLowerSiblingAsync();
            if(success)
            {
                if (lowerSiblingPath == string.Empty)
                    return (ElectionResult.IsLeader, string.Empty);
                else
                    return (ElectionResult.IsFollower, lowerSiblingPath);
            }
            else
            {
                return (ElectionResult.Error, string.Empty);
            }
        }
        
        private async Task<(bool, string)> FindLowerSiblingAsync()
        {
            try
            {
                var maxClientNumber = -1;
                var watchChild = string.Empty;
                var clients = await this.zooKeeperService.GetActiveClientsAsync();
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
                    return (true, string.Empty);

                return (true, watchChild);
            }
            catch (ZkInvalidOperationException e)
            {
                this.logger.Error(this.clientId, "Unable to determine if there are sibling clients with a lower id", e);
                return (false, string.Empty);
            }
        }

        private async Task<CoordinatorExitReason> BecomeCoordinatorAsync(CancellationToken token)
        {
            this.logger.Info(this.clientId, $"Becoming coordinator");
            ICoordinator coordinator;
            switch (this.rebalancingMode)
            {
                case RebalancingMode.GlobalBarrier:
                    coordinator = new GB.Coordinator(this.zooKeeperService,
                        this.logger,
                        this.store,
                        this.clientId,
                        this.minimumRebalancingInterval,
                        token);
                    break;
                case RebalancingMode.ResourceBarrier:
                    coordinator = new RB.Coordinator(this.zooKeeperService,
                        this.logger,
                        this.store,
                        this.clientId,
                        this.minimumRebalancingInterval,
                        token);
                    break;
                default:
                    coordinator = new RB.Coordinator(this.zooKeeperService,
                        this.logger,
                        this.store,
                        this.clientId,
                        this.minimumRebalancingInterval,
                        token);
                    break;
            }
            
            var hasBecome = await coordinator.BecomeCoordinatorAsync(this.epoch);
            switch (hasBecome)
            {
                case BecomeCoordinatorResult.Ok:
                    this.logger.Info(this.clientId, $"Have successfully become the coordinator");
                    
                    // this blocks until coordinator terminates (due to failure, session expiry or detects it is a zombie)
                    var coordinatorExitReason = await coordinator.StartEventLoopAsync();
                    this.logger.Info(this.clientId, $"The coordinator has exited for reason {coordinatorExitReason}");
                    return coordinatorExitReason;
                case BecomeCoordinatorResult.StaleEpoch:
                    this.logger.Info(this.clientId, 
                        "Since being elected, the epoch has been incremented suggesting another leader. Aborting coordinator role to check leadership again");
                    return CoordinatorExitReason.NoLongerCoordinator;
                default:
                    this.logger.Error(this.clientId, "Could not become coordinator");
                    return CoordinatorExitReason.PotentialInconsistentState;
            }
        }
        
        private async Task<FollowerExitReason> BecomeFollowerAsync(CancellationToken token)
        {
            this.logger.Info(this.clientId, $"Becoming a follower");

            IFollower follower;
            switch (this.rebalancingMode)
            {
                case RebalancingMode.GlobalBarrier:
                    follower = new GB.Follower(this.zooKeeperService,
                        this.logger,
                        this.store,
                        this.clientId,
                        this.clientNumber,
                        this.watchSiblingNodePath,
                        token);
                    break;
                case RebalancingMode.ResourceBarrier:
                    follower = new RB.Follower(this.zooKeeperService,
                        this.logger,
                        this.store,
                        this.clientId,
                        this.clientNumber,
                        this.watchSiblingNodePath,
                        token);
                    break;
                default:
                    follower = new RB.Follower(this.zooKeeperService,
                        this.logger,
                        this.store,
                        this.clientId,
                        this.clientNumber,
                        this.watchSiblingNodePath,
                        token);
                    break;
            }

            var hasBecome = await follower.BecomeFollowerAsync();
            switch(hasBecome)
            {
                case BecomeFollowerResult.Ok:
                    this.logger.Info(this.clientId, $"Have become a follower, starting follower event loop");
                
                    // blocks until follower either fails, the session expires or the follower detects it might be the new leader
                    var followerExitReason = await follower.StartEventLoopAsync();
                    this.logger.Info(this.clientId, $"The follower has exited for reason {followerExitReason}");
                    return followerExitReason;
                case BecomeFollowerResult.WatchSiblingGone:
                    this.logger.Info(this.clientId, $"The follower was unable to watch its sibling as the sibling has gone");
                    return FollowerExitReason.PossibleRoleChange;
                default:
                    this.logger.Error(this.clientId, "Could not become a follower");
                    return FollowerExitReason.PotentialInconsistentState;
            }
        }

        private async Task TerminateAsync(string terminationReason)
        {
            try
            {
                this.state = ClientStates.Terminated;
                logger.Info(this.clientId, "Client terminating due " + terminationReason);
                await this.zooKeeperService.CloseSessionAsync();
                await this.store.InvokeOnStopActionsAsync(this.clientId, "No role");
                logger.Info(this.clientId, "Client terminated");
            }
            catch (TerminateClientException e)
            {
                logger.Error(this.clientId, "Client termination failure during invocation of on stop actions", e);
            }
        }

        private async Task<bool> InvokeOnErrorActionsAsync(string message, Exception ex = null)
        {
            try
            {
                await this.store.InvokeOnErrorActionsAsync(this.clientId, message, ex);
                return true;
            }
            catch (TerminateClientException e)
            {
                logger.Error(this.clientId, "End user code threw an exception during invocation of on error event handler", e);
                return false;
            }
        }

        private async Task WaitRandomTime(TimeSpan maxWait)
        {
            await Task.Delay(this.rand.Next((int) maxWait.TotalMilliseconds));
        }
    }
}