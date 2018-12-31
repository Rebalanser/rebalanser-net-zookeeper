using System;

namespace Rebalanser.ZooKeeper.Zk
{
    public class ZkStaleVersionException: Exception
    {
        public ZkStaleVersionException(string message)
            : base(message)
        {
                
        }
        
        public ZkStaleVersionException(string message, Exception ex)
            : base(message, ex)
        {
                
        }
        
    }
}