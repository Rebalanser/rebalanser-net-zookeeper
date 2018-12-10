namespace Rebalanser.ZooKeeper
{
    public enum FollowerStatus
    {
        Ok,
        Cancelled,
        SessionExpired,
        IsNewLeader,
        UnexpectedFailure
    }
}