namespace Rebalanser.ZooKeeper
{
    public enum FollowerExitReason
    {
        NoExit,
        Cancelled,
        SessionExpired,
        IsNewLeader,
        UnexpectedFailure
    }
}