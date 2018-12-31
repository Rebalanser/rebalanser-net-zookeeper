using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using org.apache.zookeeper;
using Rebalanser.Core.Logging;

namespace Rebalanser.ZooKeeper.Zk
{
    public class ZooKeeperService : Watcher, IZooKeeperService
    {
        private org.apache.zookeeper.ZooKeeper zookeeper;
        private string zookeeperHosts;
        private string clientsPath;
        private string statusPath;
        private string stoppedPath;
        private string resourcesPath;
        private string epochPath;
        private Event.KeeperState keeperState;
        private CancellationToken token;
        private string clientId;

        public ZooKeeperService(string zookeeperHosts)
        {
            this.zookeeperHosts = zookeeperHosts;
            this.clientId = "-";
        }
        
        public async Task InitializeGlobalBarrierAsync(string clientsPath,
            string statusPath,
            string stoppedPath,
            string resourcesPath,
            string epochPath)
        {
            this.clientsPath = clientsPath;
            this.statusPath = statusPath;
            this.stoppedPath = stoppedPath;
            this.resourcesPath = resourcesPath;
            this.epochPath = epochPath;

            await EnsurePathAsync(this.clientsPath);
            await EnsurePathAsync(this.epochPath);
            await EnsurePathAsync(this.statusPath);
            await EnsurePathAsync(this.stoppedPath);
            await EnsurePathAsync(this.resourcesPath);
        }
        
        public async Task InitializeResourceBarrierAsync(string clientsPath,
            string resourcesPath,
            string epochPath)
        {
            this.clientsPath = clientsPath;
            this.resourcesPath = resourcesPath;
            this.epochPath = epochPath;

            await EnsurePathAsync(this.clientsPath);
            await EnsurePathAsync(this.epochPath);
            await EnsurePathAsync(this.resourcesPath);
        }

        public Event.KeeperState GetKeeperState()
        {
            return this.keeperState;
        }

        public async Task<bool> StartSessionAsync(TimeSpan sessionTimeout, TimeSpan connectTimeout, CancellationToken token)
        {
            this.token = token;
            var sw = new Stopwatch();
            sw.Start();
            
            if (this.zookeeper != null)
                await this.zookeeper.closeAsync();

            this.zookeeper = new org.apache.zookeeper.ZooKeeper(
                this.zookeeperHosts, 
                (int)sessionTimeout.TotalMilliseconds,
                this);

            while (this.keeperState != Event.KeeperState.SyncConnected && sw.Elapsed <= connectTimeout)
                await Task.Delay(50);

            return this.keeperState == Event.KeeperState.SyncConnected;
        }

        public async Task CloseSessionAsync()
        {
            if(this.zookeeper != null)
                await this.zookeeper.closeAsync();
            this.zookeeper = null;
        }

        public override async Task process(WatchedEvent @event)
        {
            this.keeperState = @event.getState();
            await Task.Yield();
        }

        public async Task<string> CreateClientAsync()
        {
            var actionToPerform = "create client znode";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);
                
                try
                {
                    var clientPath = await this.zookeeper.createAsync(
                        $"{this.clientsPath}/client_",
                        System.Text.Encoding.UTF8.GetBytes("0"),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);

                    this.clientId = clientPath.Substring(clientPath.LastIndexOf("/") + 1);

                    return clientPath;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as parent node does not exist", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task DeleteClientAsync(string clientPath)
        {
            var actionToPerform = "delete client znode";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await this.zookeeper.deleteAsync(clientPath);
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, will try again in the next iteration
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired.", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task EnsurePathAsync(string znodePath)
        {
            var actionToPerform = $"ensure path {znodePath}";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var znodeStat = await this.zookeeper.existsAsync(znodePath);
                    if (znodeStat == null)
                    {
                        await this.zookeeper.createAsync(znodePath,
                            System.Text.Encoding.UTF8.GetBytes("0"),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                    }

                    succeeded = true;
                }
                catch (KeeperException.NodeExistsException)
                {
                    succeeded = true; // the node exists which is what we want
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, will try again in the next iteration
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired.", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task<int> IncrementAndWatchEpochAsync(int currentEpoch, Watcher watcher)
        {
            var actionToPerform = "increment epoch";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var data = System.Text.Encoding.UTF8.GetBytes("0");
                    var stat = await zookeeper.setDataAsync(this.epochPath, data, currentEpoch);
                    
                    var dataRes = await zookeeper.getDataAsync(this.epochPath, watcher);
                    if (dataRes.Stat.getVersion() == stat.getVersion())
                        return dataRes.Stat.getVersion();
                    else
                        throw new ZkStaleVersionException("Between incrementing the epoch and setting a watch the epoch was incremented");
                }
                catch (KeeperException.BadVersionException e)
                {
                    throw new ZkStaleVersionException($"Could not {actionToPerform} as the current epoch was incremented already.", e);
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task<int> GetEpochAsync()
        {
            var actionToPerform = "get the current epoch";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var dataResult = await zookeeper.getDataAsync(this.epochPath);
                    return dataResult.Stat.getVersion();
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<ClientsZnode> GetActiveClientsAsync()
        {
            var actionToPerform = "get the list of active clients";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var childrenResult = await this.zookeeper.getChildrenAsync(this.clientsPath);
                    var childrenPaths = childrenResult.Children.Select(x => $"{this.clientsPath}/{x}").ToList();
                    return new ClientsZnode()
                    {
                        Version = childrenResult.Stat.getVersion(),
                        ClientPaths = childrenPaths
                    };
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the clients node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<StatusZnode> GetStatusAsync()
        {
            var actionToPerform = "get the status znode";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var dataResult = await zookeeper.getDataAsync(this.statusPath);
                    var status = RebalancingStatus.NotSet;
                    if (dataResult.Stat.getDataLength() > 0)
                        status = (RebalancingStatus) BitConverter.ToInt32(dataResult.Data, 0);

                    return new StatusZnode()
                    {
                        RebalancingStatus = status,
                        Version = dataResult.Stat.getVersion()
                    };
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> SetStatus(StatusZnode statusZnode)
        {
            var actionToPerform = "set the status znode";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var data = BitConverter.GetBytes((int) statusZnode.RebalancingStatus);
                    var stat = await zookeeper.setDataAsync(this.statusPath, data, statusZnode.Version);
                    return stat.getVersion();
                }
                catch (KeeperException.BadVersionException e)
                {
                    throw new ZkStaleVersionException($"Could not {actionToPerform} due to a bad version number.", e);
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task SetFollowerAsStopped(string clientId)
        {
            var actionToPerform = $"set follower {clientId} as stopped";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await this.zookeeper.createAsync(
                        $"{this.stoppedPath}/{clientId}",
                        System.Text.Encoding.UTF8.GetBytes("0"),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);

                    succeeded = true;
                }
                catch (KeeperException.NodeExistsException)
                {
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the stopped znode does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task SetFollowerAsStarted(string clientId)
        {
            var actionToPerform = $"set follower {clientId} as started";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await this.zookeeper.deleteAsync($"{this.stoppedPath}/{clientId}");
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException)
                {
                    succeeded = true;
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<ResourcesZnode> GetResourcesAsync(Watcher childWatcher, Watcher dataWatcher)
        {
            var actionToPerform = "get the list of resources";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    DataResult dataResult = null;
                    if(dataWatcher != null)
                        dataResult = await this.zookeeper.getDataAsync(this.resourcesPath, dataWatcher);
                    else
                        dataResult = await this.zookeeper.getDataAsync(this.resourcesPath);

                    ChildrenResult childrenResult = null;
                    if(childWatcher != null)
                        childrenResult = await this.zookeeper.getChildrenAsync(this.resourcesPath, childWatcher);
                    else
                        childrenResult = await this.zookeeper.getChildrenAsync(this.resourcesPath);
                    
                    var resourcesZnodeData = JsonConvert.DeserializeObject<ResourcesZnodeData>(
                        System.Text.Encoding.UTF8.GetString(dataResult.Data));

                    if (resourcesZnodeData == null)
                        resourcesZnodeData = new ResourcesZnodeData();

                    return new ResourcesZnode()
                    {
                        ResourceAssignments = resourcesZnodeData,
                        Resources = childrenResult.Children,
                        Version = dataResult.Stat.getVersion()
                    };
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException(
                        $"Could not {actionToPerform} as the resources node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task<int> SetResourcesAsync(ResourcesZnode resourcesZnode)
        {
            var actionToPerform = "set resource assignments";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var data = System.Text.Encoding.UTF8.GetBytes(
                        JsonConvert.SerializeObject(resourcesZnode.ResourceAssignments));
                    var stat = await zookeeper.setDataAsync(this.resourcesPath, data, resourcesZnode.Version);
                    return stat.getVersion();
                }
                catch (KeeperException.BadVersionException e)
                {
                    throw new ZkStaleVersionException($"Could not {actionToPerform} due to a bad version number.", e);
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task RemoveResourceBarrierAsync(string resource)
        {
            var actionToPerform = $"remove resource barrier on {resource}";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await this.zookeeper.deleteAsync($"{this.resourcesPath}/{resource}/barrier");
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException)
                {
                    succeeded = true;
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task TryPutResourceBarrierAsync(string resource, CancellationToken waitToken, ILogger logger)
        {
            var sw = new Stopwatch();
            sw.Start();
            var actionToPerform = $"try put resource barrier on {resource}";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await this.zookeeper.createAsync(
                        $"{this.resourcesPath}/{resource}/barrier",
                        System.Text.Encoding.UTF8.GetBytes(this.clientId),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                    succeeded = true;
                }
                catch (KeeperException.NodeExistsException)
                {
                    var (exists, owner) = await GetResourceBarrierOwnerAsync(resource);
                    if (exists && owner.Equals(this.clientId))
                    {
                        succeeded = true;
                    }
                    else
                    {
                        logger.Info(this.clientId, $"Waiting for {owner} to release its barrier on {resource}");
                        // wait for two seconds, will retry in next iteration
                        for (int i = 0; i < 20; i++)
                        {
                            await WaitFor(TimeSpan.FromMilliseconds(100));
                            if (waitToken.IsCancellationRequested)
                                throw new ZkOperationCancelledException(
                                    $"Could not {actionToPerform} as the operation was cancelled.");
                        }
                    }
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the resource node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        private async Task<(bool, string)> GetResourceBarrierOwnerAsync(string resource)
        {
            var actionToPerform = "get resource barrier owner";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var dataResult = await zookeeper.getDataAsync($"{this.resourcesPath}/{resource}/barrier");
                    return (true, System.Text.Encoding.UTF8.GetString(dataResult.Data));
                }
                catch (KeeperException.NoNodeException)
                {
                    return (false, string.Empty);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task<List<string>> GetStoppedAsync()
        {
            var actionToPerform = "get the list of stopped clients";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var childrenResult = await this.zookeeper.getChildrenAsync(this.stoppedPath);
                    return childrenResult.Children;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException(
                        $"Could not {actionToPerform} as the stopped node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> WatchEpochAsync(Watcher watcher)
        {
            var actionToPerform = "set a watch on epoch";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var stat = await zookeeper.existsAsync(this.epochPath, watcher);
                    return stat.getVersion();
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the epoch node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task<StatusZnode> WatchStatusAsync(Watcher watcher)
        {
            var actionToPerform = "set a watch on status";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var dataResult = await zookeeper.getDataAsync(this.statusPath, watcher);
                    return new StatusZnode()
                    {
                        RebalancingStatus = (RebalancingStatus) BitConverter.ToInt32(dataResult.Data, 0),
                        Version = dataResult.Stat.getVersion()
                    };
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the status node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task WatchResourcesChildrenAsync(Watcher watcher)
        {
            var actionToPerform = "set a watch on resource children";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await this.zookeeper.getChildrenAsync(this.resourcesPath, watcher);
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the resources node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> WatchResourcesDataAsync(Watcher watcher)
        {
            var actionToPerform = "set a watch on resource data";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    var data = await this.zookeeper.getDataAsync(this.resourcesPath, watcher);
                    return data.Stat.getVersion();
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException(
                        $"Could not {actionToPerform} as the resources node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task WatchNodesAsync(Watcher watcher)
        {
            var actionToPerform = "set a watch on clients children";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await this.zookeeper.getChildrenAsync(this.clientsPath, watcher);
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the clients node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }
        
        public async Task WatchSiblingNodeAsync(string siblingPath, Watcher watcher)
        {
            var actionToPerform = "set a watch on sibling client";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await this.zookeeper.getDataAsync(siblingPath, watcher);
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkNoEphemeralNodeWatchException($"Could not {actionToPerform} as the client node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        private async Task BlockUntilConnected(string logAction)
        {
            while (!this.token.IsCancellationRequested && this.keeperState != Event.KeeperState.SyncConnected)
            {
                if(this.keeperState == Event.KeeperState.Expired)
                    throw new ZkSessionExpiredException($"Could not {logAction} because the session has expired");
                
                await WaitFor(TimeSpan.FromMilliseconds(100));
            }

            if (this.token.IsCancellationRequested)
                throw new ZkOperationCancelledException($"Could not {logAction} because the operation was cancelled");
        }
        
        private async Task BlockUntilConnected(string logAction, CancellationToken waitingToken)
        {
            while (!this.token.IsCancellationRequested
                   && !waitingToken.IsCancellationRequested
                   && this.keeperState != Event.KeeperState.SyncConnected)
            {
                if(this.keeperState == Event.KeeperState.Expired)
                    throw new ZkSessionExpiredException($"Could not {logAction} because the session has expired");
                
                await WaitFor(TimeSpan.FromMilliseconds(100));
            }

            if (this.token.IsCancellationRequested || waitingToken.IsCancellationRequested)
                throw new ZkOperationCancelledException($"Could not {logAction} because the operation was cancelled");
        }

        private async Task WaitFor(TimeSpan waitPeriod)
        {
            try
            {
                await Task.Delay(waitPeriod, this.token);
            }
            catch (TaskCanceledException)
            {}
        }
        
        private async Task WaitFor(TimeSpan waitPeriod, CancellationToken waitToken)
        {
            try
            {
                await Task.Delay(waitPeriod, waitToken);
            }
            catch (TaskCanceledException)
            {}
        }
    }
}