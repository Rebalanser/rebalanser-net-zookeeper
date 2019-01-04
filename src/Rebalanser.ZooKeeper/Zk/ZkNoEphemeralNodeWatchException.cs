using System;

namespace Rebalanser.ZooKeeper.Zk
{
    public class ZkNoEphemeralNodeWatchException : Exception
    {
        public ZkNoEphemeralNodeWatchException(string message)
        : base(message)
        {
                
        }
        
        public ZkNoEphemeralNodeWatchException(string message, Exception ex)
            : base(message, ex)
        {
                
        }
    }
}