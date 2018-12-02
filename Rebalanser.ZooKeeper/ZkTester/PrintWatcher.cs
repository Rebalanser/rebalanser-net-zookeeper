using System;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace ZkTester
{
    public class PrintWatcher : Watcher
    {
        private string msg;

        public PrintWatcher(string msg)
        {
            this.msg = msg;
        }
        
        public override Task process(WatchedEvent @event)
        {
            Console.WriteLine("EVENT!! " + msg + ": " + @event.ToString());
            
            return Task.CompletedTask;
        }
    }
}