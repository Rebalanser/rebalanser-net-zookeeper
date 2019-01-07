using System;
using System.Diagnostics;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace QueueConfig
{
    public class ZkHelper : Watcher
    {
        public string zooKeeperHosts;
        private org.apache.zookeeper.ZooKeeper zookeeper;
        private string zkRootPath;
        private Event.KeeperState keeperState;
        private TimeSpan sessionTimeout;
        private TimeSpan connectTimeout;
        
        
        public async Task InitializeAsync(string zooKeeperHosts, string zkRootPath, TimeSpan sessionTimeout, TimeSpan connectTimeout)
        {
            this.zooKeeperHosts = zooKeeperHosts;
            this.zkRootPath = zkRootPath;
            this.sessionTimeout = sessionTimeout;
            this.connectTimeout = connectTimeout;
            await EstablishSession();
        }

        private async Task EstablishSession()
        {
            this.zookeeper = new org.apache.zookeeper.ZooKeeper(
                this.zooKeeperHosts, 
                (int)this.sessionTimeout.TotalMilliseconds,
                this);

            var sw = new Stopwatch();
            sw.Start();
            while (this.keeperState != Event.KeeperState.SyncConnected && sw.Elapsed < this.connectTimeout)
                await Task.Delay(50);
    
            if(this.keeperState != Event.KeeperState.SyncConnected)
                throw new Exception("Could not establish test session");
            
            await EnsureZnodeAsync(this.zkRootPath);
        }

        public async Task CloseAsync()
        {
            if(this.zookeeper != null)
                await this.zookeeper.closeAsync();
        }
        
        public async Task PrepareResourceGroupAsync(string group, string resourcePrefix, int count)
        {
            await this.CreateZnodeAsync($"{zkRootPath}/{group}");
            await this.CreateZnodeAsync($"{zkRootPath}/{group}/resources");
            
            for (int i = 1; i <= count; i++)
            {
                var path = $"{zkRootPath}/{group}/resources/{resourcePrefix}_{i.ToString().PadLeft(5, '0')}";
                await CreateZnodeAsync(path);
                Console.Write($"Added queue {path} in Apache ZooKeeper\r\n");
            }

            await DeleteResourcesAfterMax(group, count);
        }
        
        public async Task DeleteResourcesAsync(string group, string resourcePrefix, int count)
        {
            for (int i = 0; i < count; i++)
                await SafeDelete($"{zkRootPath}/{group}/resources/{resourcePrefix}{i.ToString().PadLeft(5, '0')}");
        }
        
        public async Task AddResourceAsync(string group, string resourceName)
        {
            await CreateZnodeAsync($"{zkRootPath}/{group}/resources/{resourceName}");
        }
        
        public async Task DeleteResourceAsync(string group, string resourceName)
        {
            while (true)
            {
                try
                {
                    var childrenRes = await zookeeper.getChildrenAsync($"{zkRootPath}/{group}/resources/{resourceName}");
                    foreach (var child in childrenRes.Children)
                        await SafeDelete($"{zkRootPath}/{group}/resources/{resourceName}/{child}");

                    await SafeDelete($"{zkRootPath}/{group}/resources/{resourceName}");
                    return;
                }
                catch (KeeperException.NoNodeException)
                {
                    return;
                }
                catch (KeeperException.ConnectionLossException)
                {

                }
                catch (KeeperException.SessionExpiredException)
                {
                    await EstablishSession();
                }
            }
        }
        
        public async Task DeleteResourcesAfterMax(string group, int maxCount)
        {
            while (true)
            {
                try
                {
                    var childrenRes = await zookeeper.getChildrenAsync($"{zkRootPath}/{group}/resources");
                    foreach (var child in childrenRes.Children)
                    {
                        var indexStr = child.Substring(child.LastIndexOf("_")+1);
                        int index = int.Parse(indexStr);
                        if(index > maxCount)
                            await SafeDelete($"{zkRootPath}/{group}/resources/{child}");
                    }

                    return;
                }
                catch (KeeperException.NoNodeException)
                {
                    return;
                }
                catch (KeeperException.ConnectionLossException)
                {

                }
                catch (KeeperException.SessionExpiredException)
                {
                    await EstablishSession();
                }
            }
        }

        private async Task SafeDelete(string path)
        {
            while (true)
            {
                try
                {
                    await zookeeper.deleteAsync(path);
                    Console.Write($"Removed queue {path} in Apache ZooKeeper\r\n");
                    return;
                }
                catch (KeeperException.NoNodeException)
                {
                    return;
                }
                catch (KeeperException.NotEmptyException)
                {
                    return;
                }
                catch (KeeperException.ConnectionLossException)
                {

                }
                catch (KeeperException.SessionExpiredException)
                {
                    await EstablishSession();
                }
            }
        }

        public override async Task process(WatchedEvent @event)
        {
            this.keeperState = @event.getState();
            await Task.Yield();
        }

        private async Task EnsureZnodeAsync(string path)
        {
            while (true)
            {
                try
                {
                    await this.zookeeper.createAsync(path,
                        System.Text.Encoding.UTF8.GetBytes("0"),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                    return;
                }
                catch (KeeperException.NodeExistsException)
                {
                    return;
                }
                catch (KeeperException.ConnectionLossException)
                {

                }
                catch (KeeperException.SessionExpiredException)
                {
                    await EstablishSession();
                }
            }
        }
        
        private async Task CreateZnodeAsync(string path)
        {
            while (true)
            {
                try
                {
                    await this.zookeeper.createAsync(path,
                        new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);

                    return;
                }
                catch (KeeperException.NodeExistsException)
                {
                    return;
                }
                catch (KeeperException.ConnectionLossException)
                {

                }
                catch (KeeperException.SessionExpiredException)
                {
                    await EstablishSession();
                }
            }
        }
}
}