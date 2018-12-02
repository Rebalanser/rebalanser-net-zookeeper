namespace Rebalanser.ZooKeeper.Zk
{
    public enum ZkResult
    {
        Ok,
        SessionExpired,
        NotAuthorized,
        BadVersion,
        NoZnode,
        ConnectionLost,
        UnexpectedError
    }
}