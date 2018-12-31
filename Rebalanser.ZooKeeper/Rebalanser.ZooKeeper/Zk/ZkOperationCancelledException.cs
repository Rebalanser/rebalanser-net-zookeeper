using System;

namespace Rebalanser.ZooKeeper.Zk
{
    public class ZkOperationCancelledException : Exception
    {
        public ZkOperationCancelledException(string message)
            : base(message)
        {
                
        }
    }
}