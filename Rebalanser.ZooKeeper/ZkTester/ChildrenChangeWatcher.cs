using System;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace ZkTester
{
    public class ChildrenChangeWatcher : Watcher
    {
        private org.apache.zookeeper.ZooKeeper zookeeper;
        private string msg;
        private string leaderZnodeRoot;
        private string path;
        private CancellationToken token;

        public ChildrenChangeWatcher(string msg,
            string leaderZnodeRoot,
            string path,
            org.apache.zookeeper.ZooKeeper zookeeper,
            CancellationToken token)
        {
            this.msg = msg;
            this.zookeeper = zookeeper;
            this.leaderZnodeRoot = leaderZnodeRoot;
            this.path = path;
            this.token = token;
        }
        
        public override async Task process(WatchedEvent @event)
        {
            if (!token.IsCancellationRequested)
            {
                Console.WriteLine("EVENT!! " + msg + ": " + @event.ToString());
                int mySeqNo = int.Parse(path.Substring(path.Length - 10, 10));

                var childrenResult = await this.zookeeper.getChildrenAsync(this.leaderZnodeRoot);

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
                }
                else
                {
                    var childWatcher = new ChildrenChangeWatcher("CHILDREN CHANGED",
                        this.leaderZnodeRoot,
                        path,
                        this.zookeeper,
                        this.token);

                    var watchChildPath = $"{this.leaderZnodeRoot}/{watchChild}";
                    Console.WriteLine($"I AM A FOLLOWER! Watch child {watchChildPath}");
                    var stat = await zookeeper.existsAsync(watchChildPath, childWatcher);
                }
            }
        }
    }
}