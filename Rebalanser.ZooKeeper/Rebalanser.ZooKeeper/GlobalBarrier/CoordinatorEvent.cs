namespace Rebalanser.ZooKeeper.GlobalBarrier
{
    public enum CoordinatorEvent
    {
        SessionExpired,
        NoLongerCoordinator,
        RebalancingTriggered,
        PotentialInconsistentState,
        FatalError
    }
}