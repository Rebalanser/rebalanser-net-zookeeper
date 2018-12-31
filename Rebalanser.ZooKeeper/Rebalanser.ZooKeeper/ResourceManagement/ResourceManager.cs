using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.Zk;

namespace Rebalanser.ZooKeeper.ResourceManagement
{
    public class ResourceManager
    {
        private ILogger logger;
        private IZooKeeperService zooKeeperService;
        private List<string> resources;
        SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1,1);
        private OnChangeActions onChangeActions;
        private ClientOptions clientOptions;
        private RebalancingMode rebalancingMode;

        public ResourceManager(IZooKeeperService zooKeeperService,
            ILogger logger,
            OnChangeActions onChangeActions,
            ClientOptions clientOptions,
            RebalancingMode rebalancingMode)
        {
            this.zooKeeperService = zooKeeperService;
            this.logger = logger;
            this.resources = new List<string>();
            AssignmentStatus = AssignmentStatus.NoAssignmentYet;
            this.onChangeActions = onChangeActions;
            this.clientOptions = clientOptions;
            this.rebalancingMode = rebalancingMode;
        }

        public AssignmentStatus AssignmentStatus { get; set; }
        
        public GetResourcesResponse GetResources()
        {
            if (AssignmentStatus == AssignmentStatus.ResourcesAssigned)
            {
                return new GetResourcesResponse()
                {
                    Resources = new List<string>(resources),
                    AssignmentStatus = AssignmentStatus
                };
            }
            else
            {
                return new GetResourcesResponse()
                {
                    Resources = new List<string>(),
                    AssignmentStatus = AssignmentStatus
                };
            }
        }

        public async Task InvokeOnStopActionsAsync(string clientId, string role)
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                var resourcesToRemove = new List<string>(this.resources);
                if (resourcesToRemove.Any())
                {
                    
                    this.logger.Info(clientId, $"{role} - Invoking on stop actions. Unassigned resources {string.Join(",", resourcesToRemove)}");
                    
                    try
                    {
                        foreach (var onStopAction in this.onChangeActions.OnStopActions)
                            onStopAction.Invoke();
                    }
                    catch (Exception e)
                    {
                        this.logger.Error(clientId, "{role} - End user on stop actions threw an exception. Terminating. ", e);
                        throw new TerminateClientException("End user on stop actions threw an exception.", e);
                    }
                
                    if (this.rebalancingMode == RebalancingMode.ResourceBarrier)
                    {
                        try
                        {
                            this.logger.Info(clientId,
                                $"{role} - Removing barriers on resources {string.Join(",", resourcesToRemove)}");
                            int counter = 1;
                            foreach (var resource in resourcesToRemove)
                            {
                                await this.zooKeeperService.RemoveResourceBarrierAsync(resource);

                                if (counter % 10 == 0)
                                {
                                    this.logger.Info(clientId,
                                        $"{role} - Removed barriers on {counter} resources of {resourcesToRemove.Count}");
                                }

                                counter++;
                            }
                                this.logger.Info(clientId, $"{role} - Removed barriers on {resourcesToRemove.Count} resources of {resourcesToRemove.Count}");
                        }
                        catch (ZkOperationCancelledException)
                        {
                            // do nothing, cancellation is in progress, these are ephemeral nodes anyway
                        }
                        catch(ZkSessionExpiredException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            throw new InconsistentStateException("An error occurred while removing resource barriers", e);
                        }
                    }
                    
                    this.resources.Clear();
                    this.logger.Info(clientId, $"{role} - On stop complete");
                    AssignmentStatus = AssignmentStatus.ResourcesAssigned;
                }
                else
                {
                    AssignmentStatus = AssignmentStatus.NoResourcesAssigned;
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }
        
        public async Task InvokeOnStartActionsAsync(string clientId, 
            string role, 
            List<string> newResources, 
            CancellationToken rebalancingToken,
            CancellationToken clientToken)
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                this.resources = new List<string>(newResources);
                
                if (this.resources.Any())
                {
                    if (this.rebalancingMode == RebalancingMode.ResourceBarrier)
                    {
                        try
                        {
                            this.logger.Info(clientId,
                                $"{role} - Putting barriers on resources {string.Join(",", newResources)}");
                            int counter = 1;
                            foreach (var resource in newResources)
                            {
                                await this.zooKeeperService.TryPutResourceBarrierAsync(resource, rebalancingToken,
                                    this.logger);
                                
                                if (counter % 10 == 0)
                                {
                                    this.logger.Info(clientId,
                                        $"{role} - Put barriers on {counter} resources of {newResources.Count}");
                                }

                                counter++;
                            }
                            this.logger.Info(clientId, $"{role} - Put barriers on {newResources.Count} resources of {newResources.Count}");
                        }
                        catch (ZkOperationCancelledException)
                        {
                            if (clientToken.IsCancellationRequested)
                            {
                                throw;
                            }
                            else
                            {
                                this.logger.Info(clientId,
                                    $"{role} - Rebalancing cancelled, removing barriers on resources {string.Join(",", newResources)}");
                                try
                                {
                                    int counter = 1;
                                    foreach (var resource in newResources)
                                    {
                                        await this.zooKeeperService.RemoveResourceBarrierAsync(resource);
                                        
                                        if (counter % 10 == 0)
                                        {
                                            this.logger.Info(clientId,
                                                $"{role} - Removing barriers on {counter} resources of {newResources.Count}");
                                        }

                                        counter++;
                                    }
                                    this.logger.Info(clientId, $"{role} - Removed barriers on {newResources.Count} resources of {newResources.Count}");
                                }
                                catch(ZkSessionExpiredException)
                                {
                                    throw;
                                }
                                catch (ZkOperationCancelledException)
                                {
                                    // do nothing, client cancellation in progress
                                }
                                catch (Exception e)
                                {
                                    throw new InconsistentStateException(
                                        "An error occurred while removing resource barriers due to rebalancing cancellation", e);
                                }

                                return;
                            }
                        }
                        catch(ZkSessionExpiredException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            throw new InconsistentStateException("An error occurred while putting resource barriers", e);
                        }
                    }
    
                    try
                    {
                        this.logger.Info(clientId, $"{role} - Invoking on start with resources {string.Join(",", this.resources)}");
                        foreach (var onStartAction in this.onChangeActions.OnStartActions)
                            onStartAction.Invoke(this.resources);
                    }
                    catch (Exception e)
                    {
                        this.logger.Error(clientId, $"{role} - End user on start actions threw an exception. Terminating. ", e);
                        throw new TerminateClientException("End user on start actions threw an exception.", e);
                    }
                    
                    this.logger.Info(clientId, $"{role} - On start complete");
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }
        
        
        public async Task InvokeOnErrorActionsAsync(string clientId, string message, Exception ex = null)
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                this.logger.Info(clientId, "Invoking on error actions.");
                    
                try
                {
                    foreach (var onErrorAction in this.onChangeActions.OnErrorActions)
                        onErrorAction.Invoke(message, this.clientOptions.AutoRecoveryOnError, ex);
                }
                catch (Exception e)
                {
                    this.logger.Error(clientId, "End user on error actions threw an exception. Terminating. ", e);
                    throw new TerminateClientException("End user on error actions threw an exception.", e);
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }
        
        public async Task<bool> SafeInvokeOnErrorActionsAsync(string clientId, string message, Exception ex = null)
        {
            try
            {
                await InvokeOnErrorActionsAsync(clientId, message, ex);
                return true;
            }
            catch (TerminateClientException e)
            {
                logger.Error(clientId, "End user code threw an exception during invocation of on error event handler", e);
                return false;
            }
        }
    }
}