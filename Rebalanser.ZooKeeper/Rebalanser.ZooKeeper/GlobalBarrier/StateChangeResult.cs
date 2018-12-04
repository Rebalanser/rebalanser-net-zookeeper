namespace Rebalanser.ZooKeeper.GlobalBarrier
{
    public class StateChangeResult
    {
        public StateChangeResult()
        {
        }
        
        public StateChangeResult(FollowerExitReason exitReason)
        {
            ExitReason = exitReason;
        }
        
        public FollowerExitReason ExitReason { get; set; }
        public int LastStopVersion { get; set; }
        public int LastStartVersion { get; set; }
    }
}