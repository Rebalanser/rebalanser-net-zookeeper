using System.Threading.Tasks;

namespace Rebalanser.ZooKeeper
{
    public interface IFollower
    {
        Task<bool> BecomeFollowerAsync();
        Task<FollowerExitReason> StartEventLoopAsync();
    }
}