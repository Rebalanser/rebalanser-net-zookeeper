namespace Rebalanser.ZooKeeper
{
    public enum SessionStatus
    {
        Valid,
        Cancelled,
        Expired,
        CouldNotEstablishSession,
        NonRecoverableError
    }
}