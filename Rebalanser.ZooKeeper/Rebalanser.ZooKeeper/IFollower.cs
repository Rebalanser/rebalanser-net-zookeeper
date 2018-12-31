using System.Threading.Tasks;
using Rebalanser.ZooKeeper.Zk;

namespace Rebalanser.ZooKeeper
{
    public interface IFollower
    {
        Task<BecomeFollowerResult> BecomeFollowerAsync();
        Task<FollowerExitReason> StartEventLoopAsync();
    }
}