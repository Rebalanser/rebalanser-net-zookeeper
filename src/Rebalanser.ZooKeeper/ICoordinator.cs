using System.Threading.Tasks;

namespace Rebalanser.ZooKeeper
{
    public interface ICoordinator
    {
        Task<BecomeCoordinatorResult> BecomeCoordinatorAsync(int currentEpoch);
        Task<CoordinatorExitReason> StartEventLoopAsync();
    }
}