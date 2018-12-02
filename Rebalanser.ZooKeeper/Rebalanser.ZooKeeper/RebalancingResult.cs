namespace Rebalanser.ZooKeeper
{
    public enum RebalancingResult
    {
        Complete,
        Cancelled,
        NotCoordinator,
        TimeLimitExceeded,
        SessionExpired,
        Failure
    }
}