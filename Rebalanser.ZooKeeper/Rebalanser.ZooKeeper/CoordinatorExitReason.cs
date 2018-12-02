namespace Rebalanser.ZooKeeper
{
    public enum CoordinatorExitReason
    {
        NoExit,
        NotCoordinator,
        Cancelled,
        SessionExpired,
        RebalancingError,
        Unknown
    }
}