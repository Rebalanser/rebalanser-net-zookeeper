using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace Rebalanser.ZooKeeper.Zk
{
    public interface IZooKeeperService
    {
        Watcher.Event.KeeperState GetKeeperState();
        Task StartSessionAsync(TimeSpan sessionTimeout);
        Task CloseSessionAsync();
        Task<bool> InitializeAsync(string clientsPath,
            string statusPath,
            string stoppedPath,
            string resourcesPath,
            string epochPath);
        Task<ZkResult> DeleteClientAsync(string clientPath);
        Task<ZkResponse<string>> CreateClientAsync();
        Task<ZkResult> EnsurePathAsync(string znodePath);
        Task<ZkResponse<int>> IncrementEpochAsync(int currentEpoch);
        Task<ZkResponse<int>> GetEpochAsync();
        Task<ZkResponse<ClientsZnode>> GetActiveClientsAsync();
        Task<ZkResponse<StatusZnode>> GetStatusAsync();
        Task<ZkResponse<int>> SetStatus(StatusZnode statusZnode);
        Task<ZkResult> SetFollowerAsStopped(string clientId);
        Task<ZkResult> SetFollowerAsStarted(string clientId);
        Task<ZkResponse<ResourcesZnode>> GetResourcesAsync();
        Task<ZkResponse<int>> SetResourcesAsync(ResourcesZnode resourcesZnode);
        Task<ZkResponse<List<string>>> GetStoppedAsync();
        Task<ZkResponse<int>> WatchEpochAsync(Watcher watcher);
        Task<ZkResponse<StatusZnode>> WatchStatusAsync(Watcher watcher);
        Task<ZkResult> WatchResourcesAsync(Watcher watcher);
        Task<ZkResult> WatchNodesAsync(Watcher watcher);
        Task<ZkResult> WatchSiblingNodeAsync(string siblingPath, Watcher watcher);
    }
}