namespace Rebalanser.ZooKeeper
{
    public enum ClientStates
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