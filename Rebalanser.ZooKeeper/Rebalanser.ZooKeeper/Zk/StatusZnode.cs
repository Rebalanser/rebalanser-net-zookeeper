namespace Rebalanser.ZooKeeper.Zk
{
    public class StatusZnode
    {
        public CoordinatorStatus CoordinatorStatus { get; set; }
        public int Version { get; set; }
    }
}