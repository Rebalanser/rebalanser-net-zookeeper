namespace Rebalanser.ZooKeeper.GlobalBarrier
{
    public class StateChangeResult
    {
        public StateChangeResult()
        {
        }
        
        public StateChangeResult(FollowerStatus exitReason)
        {
            ExitReason = exitReason;
        }
        
        public FollowerStatus ExitReason { get; set; }
        public int LastStopVersion { get; set; }
        public int LastStartVersion { get; set; }
    }
}