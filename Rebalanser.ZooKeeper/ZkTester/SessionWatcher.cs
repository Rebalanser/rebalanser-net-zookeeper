using System;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace ZkTester
{
    public class SessionWatcher : Watcher
    {
        private CancellationTokenSource cts;
        
        public SessionWatcher(CancellationTokenSource cts)
        {
            this.cts = cts;
        }
        
        public override Task process(WatchedEvent @event)
        {
            if (@event.getState() == Event.KeeperState.Expired)
            {
                cts.Cancel();
                Console.WriteLine("Session expired, cancellation called");
            }

            return Task.CompletedTask;
        }
    }
}