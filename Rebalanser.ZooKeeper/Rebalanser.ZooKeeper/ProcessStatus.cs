namespace Rebalanser.ZooKeeper
{
    public enum ProcessStatus
    {
        Valid,
        ErrorWithValidSession,
        Cancelled,
        ExpiredSession,
        CouldNotEstablishSession,
        NonRecoverableError,
        RoleChange
    }
}