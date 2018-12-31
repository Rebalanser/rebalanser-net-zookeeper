namespace Rebalanser.ZooKeeper.ResourceBarrier
{
    public enum FollowerEvent
    {
        SessionExpired,
        IsNewLeader,
        RebalancingTriggered,
        PotentialInconsistentState,
        FatalError
    }
}