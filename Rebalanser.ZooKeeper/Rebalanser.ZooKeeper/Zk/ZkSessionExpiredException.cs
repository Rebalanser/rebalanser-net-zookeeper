using System;

namespace Rebalanser.ZooKeeper.Zk
{
    public class ZkSessionExpiredException : Exception
    {
        public ZkSessionExpiredException(string message)
            : base(message)
        {
                
        }
        
        public ZkSessionExpiredException(string message, Exception ex)
            : base(message, ex)
        {
                
        }
    }
}