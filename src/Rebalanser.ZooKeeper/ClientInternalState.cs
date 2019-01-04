namespace Rebalanser.ZooKeeper
{
    public enum ClientInternalState
    {
        NoSession,
        NoClientNode,
        NoRole,
        Error,
        IsLeader,
        IsFollower,
        Terminated
    }
}