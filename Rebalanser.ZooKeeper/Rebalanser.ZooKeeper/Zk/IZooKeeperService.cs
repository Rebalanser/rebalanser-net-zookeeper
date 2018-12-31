using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;
using Rebalanser.Core.Logging;

namespace Rebalanser.ZooKeeper.Zk
{
    public interface IZooKeeperService
    {
        Watcher.Event.KeeperState GetKeeperState();
        Task<bool> StartSessionAsync(TimeSpan sessionTimeout, TimeSpan connectTimeout, CancellationToken token);
        Task CloseSessionAsync();
        Task InitializeGlobalBarrierAsync(string clientsPath,
            string statusPath,
            string stoppedPath,
            string resourcesPath,
            string epochPath);
        Task InitializeResourceBarrierAsync(string clientsPath,
            string resourcesPath,
            string epochPath);
        Task DeleteClientAsync(string clientPath);
        Task<string> CreateClientAsync();
        Task EnsurePathAsync(string znodePath);
        Task<int> IncrementAndWatchEpochAsync(int currentEpoch, Watcher watcher);
        Task<int> GetEpochAsync();
        Task<ClientsZnode> GetActiveClientsAsync();
        Task<StatusZnode> GetStatusAsync();
        Task<int> SetStatus(StatusZnode statusZnode);
        Task SetFollowerAsStopped(string clientId);
        Task SetFollowerAsStarted(string clientId);
        Task<ResourcesZnode> GetResourcesAsync(Watcher childWatcher, Watcher dataWatcher);
        Task<int> SetResourcesAsync(ResourcesZnode resourcesZnode);
        Task RemoveResourceBarrierAsync(string resourcePath);
        Task TryPutResourceBarrierAsync(string resourcePath, CancellationToken waitToken, ILogger logger);
        Task<List<string>> GetStoppedAsync();
        Task<int> WatchEpochAsync(Watcher watcher);
        Task<StatusZnode> WatchStatusAsync(Watcher watcher);
        Task WatchResourcesChildrenAsync(Watcher watcher);
        Task<int> WatchResourcesDataAsync(Watcher watcher);
        Task WatchNodesAsync(Watcher watcher);
        Task WatchSiblingNodeAsync(string siblingPath, Watcher watcher);
    }
}