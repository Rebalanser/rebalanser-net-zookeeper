using System;
using System.Linq;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace Rebalanser.ZooKeeper.Tests.Helpers
{
    public class ZkHelper : Watcher
    {
        private org.apache.zookeeper.ZooKeeper zookeeper;
        private string zkRootPath;
        private Event.KeeperState keeperState;
        
        public async Task InitializeAsync(string hosts, string zkRootPath, TimeSpan sessionTimeout)
        {
            this.zookeeper = new org.apache.zookeeper.ZooKeeper(
                hosts, 
                (int)sessionTimeout.TotalMilliseconds,
                this);
            this.zkRootPath = zkRootPath;
            
            while (keeperState != Event.KeeperState.SyncConnected)
                await Task.Delay(50);

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
            
            for (int i = 0; i < count; i++)
                await CreateZnodeAsync($"{zkRootPath}/{group}/resources/{resourcePrefix}{i}");
        }
        
        public async Task DeleteResourcesAsync(string group, string resourcePrefix, int count)
        {
            for (int i = 0; i < count; i++)
            {
                await zookeeper.deleteAsync($"{zkRootPath}/{group}/resources/{resourcePrefix}{i}");
            }
        }
        
        public async Task AddResourceAsync(string group, string resourceName)
        {
            await CreateZnodeAsync($"{zkRootPath}/{group}/resources/{resourceName}");
        }
        
        public async Task DeleteResourceAsync(string group, string resourceName)
        {
            var childrenRes = await zookeeper.getChildrenAsync($"{zkRootPath}/{group}/resources/{resourceName}");
            foreach(var child in childrenRes.Children)
                await zookeeper.deleteAsync($"{zkRootPath}/{group}/resources/{resourceName}/{child}");
                
            await zookeeper.deleteAsync($"{zkRootPath}/{group}/resources/{resourceName}");
        }

        public override async Task process(WatchedEvent @event)
        {
            this.keeperState = @event.getState();
            await Task.Yield();
        }

        private async Task EnsureZnodeAsync(string path)
        {
            try
            {
                await this.zookeeper.createAsync(path,
                    System.Text.Encoding.UTF8.GetBytes("0"),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            }
            catch(KeeperException.NodeExistsException){}
        }
        
        private async Task CreateZnodeAsync(string path)
        {
            await this.zookeeper.createAsync(path,
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        }
    }
}