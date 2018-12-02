namespace Rebalanser.ZooKeeper
{
    public enum SessionTermination
    {
        Cancelled,
        Expired,
        CouldNotEstablishSession,
        NonRecoverableError
    }
}