using System.Threading.Tasks;

namespace Rebalanser.ZooKeeper
{
    public interface ICoordinator
    {
        Task<bool> BecomeCoordinatorAsync();
        Task<CoordinatorExitReason> StartEventLoopAsync();
    }
}