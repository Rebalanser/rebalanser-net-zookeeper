using System;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace ZkTester
{
    public class Node
    {
        private org.apache.zookeeper.ZooKeeper zookeeper;
        private string nodesZnodeRoot;
        private CancellationTokenSource cts;
        
        public Node(string zooKeeperHost,
            string zooKeeperRootPath,
            int sessionTimeout,
            CancellationTokenSource cts)
        {
            this.nodesZnodeRoot = $"/{zooKeeperRootPath}/testgroup/nodes";
            this.zookeeper = new org.apache.zookeeper.ZooKeeper(zooKeeperHost, sessionTimeout, new SessionWatcher(cts));
            this.cts = cts;
        }

        public void Go()
        {
            Task.Run(async () => { await StartNodeAsync(cts.Token);});
        }
        
        private async Task StartNodeAsync(CancellationToken token)
        {
            var data = Guid.NewGuid().ToByteArray();
            var path = await this.zookeeper.createAsync(
                $"{nodesZnodeRoot}/n_",
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
            Console.WriteLine($"{path} created");

            int mySeqNo = int.Parse(path.Substring(path.Length - 10, 10));

            var childrenResult = await this.zookeeper.getChildrenAsync(this.nodesZnodeRoot);

            int maxSeqNo = 0;
            string watchChild = string.Empty;
            foreach (var child in childrenResult.Children)
            {
                int childSeqNo = int.Parse(child.Substring(child.Length - 10, 10));
                if (childSeqNo > maxSeqNo && childSeqNo < mySeqNo)
                {
                    watchChild = child;
                    maxSeqNo = childSeqNo;
                }
            }

            if (maxSeqNo == 0)
            {
                Console.WriteLine("I AM THE LEADER");
                // watch for session expiry
            }
            else
            {
                var childWatcher = new ChildrenChangeWatcher("CHILDREN CHANGED",
                    this.nodesZnodeRoot,
                    path,
                    this.zookeeper,
                    token);
                
                var watchChildPath = $"{this.nodesZnodeRoot}/{watchChild}";
                Console.WriteLine($"I AM A FOLLOWER! Watch child {watchChildPath}");
                var stat = await zookeeper.existsAsync(watchChildPath, childWatcher);
            }

            while (!token.IsCancellationRequested)
            {
                //int version = (await zookeeper.existsAsync(path, true)).getVersion();
                //await zookeeper.setDataAsync(path, data, version);
                //await zookeeper.getDataAsync(path);
                //Console.WriteLine($"Get data at: {path}");
                await Task.Delay(100);
            }
            Console.WriteLine("CLOSING SESSION");
            await zookeeper.closeAsync();
            Console.WriteLine("SESSION CLOSED");
        }

    }
}