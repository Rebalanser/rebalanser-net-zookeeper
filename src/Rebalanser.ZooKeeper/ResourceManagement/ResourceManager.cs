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
        // services
        private ILogger logger;
        private IZooKeeperService zooKeeperService;
        
        // immutable state
        private SemaphoreSlim actionsSemaphore = new SemaphoreSlim(1,1);
        private object resourcesLockObj = new object();
        private OnChangeActions onChangeActions;
        private RebalancingMode rebalancingMode;
        
        // mutable statwe
        private List<string> resources;
        private AssignmentStatus assignmentStatus;
        
        public ResourceManager(IZooKeeperService zooKeeperService,
            ILogger logger,
            OnChangeActions onChangeActions,
            RebalancingMode rebalancingMode)
        {
            this.zooKeeperService = zooKeeperService;
            this.logger = logger;
            this.resources = new List<string>();
            this.assignmentStatus = AssignmentStatus.NoAssignmentYet;
            this.onChangeActions = onChangeActions;
            this.rebalancingMode = rebalancingMode;
        }

        public AssignmentStatus GetAssignmentStatus()
        {
            lock (resourcesLockObj)
                return this.assignmentStatus;
        }
        
        public GetResourcesResponse GetResources()
        {
            lock (resourcesLockObj)
            {
                if (this.assignmentStatus == AssignmentStatus.ResourcesAssigned)
                {
                    return new GetResourcesResponse()
                    {
                        Resources = new List<string>(resources),
                        AssignmentStatus = this.assignmentStatus
                    };
                }
                else
                {
                    return new GetResourcesResponse()
                    {
                        Resources = new List<string>(),
                        AssignmentStatus = this.assignmentStatus
                    };
                }
            }
        }

        private void SetResources(AssignmentStatus newAssignmentStatus, List<string> newResources)
        {
            lock (resourcesLockObj)
            {
                this.assignmentStatus = newAssignmentStatus;
                this.resources = new List<string>(newResources);
            }
        }

        public bool IsInStartedState()
        {
            lock (resourcesLockObj)
            {
                return this.assignmentStatus == AssignmentStatus.ResourcesAssigned ||
                       this.assignmentStatus == AssignmentStatus.NoResourcesAssigned;
            }
        }

        public async Task InvokeOnStopActionsAsync(string clientId, string role)
        {
            await actionsSemaphore.WaitAsync();
            
            try
            {
                var resourcesToRemove = new List<string>(GetResources().Resources);
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
                    
                    SetResources(AssignmentStatus.NoAssignmentYet, new List<string>());
                    this.logger.Info(clientId, $"{role} - On stop complete");
                }
                else
                {
                    SetResources(AssignmentStatus.NoAssignmentYet, new List<string>());
                }
            }
            finally
            {
                actionsSemaphore.Release();
            }
        }
        
        public async Task InvokeOnStartActionsAsync(string clientId, 
            string role, 
            List<string> newResources, 
            CancellationToken rebalancingToken,
            CancellationToken clientToken)
        {
            await actionsSemaphore.WaitAsync();

            if (IsInStartedState())
                throw new InconsistentStateException("An attempt to invoke on start actions occurred while already in the started state");
            
            try
            {
                if (newResources.Any())
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
    
                    SetResources(AssignmentStatus.ResourcesAssigned, newResources);
                    
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
                else
                {
                    SetResources(AssignmentStatus.NoResourcesAssigned, newResources);
                }
            }
            finally
            {
                actionsSemaphore.Release();
            }
        }
        
        
        public async Task InvokeOnAbortActionsAsync(string clientId, string message, Exception ex = null)
        {
            await actionsSemaphore.WaitAsync();
            try
            {
                this.logger.Info(clientId, "Invoking on abort actions.");
                    
                try
                {
                    foreach (var onAbortAction in this.onChangeActions.OnAbortActions)
                        onAbortAction.Invoke(message, ex);
                }
                catch (Exception e)
                {
                    this.logger.Error(clientId, "End user on error actions threw an exception. Terminating. ", e);
                    throw new TerminateClientException("End user on error actions threw an exception.", e);
                }
            }
            finally
            {
                actionsSemaphore.Release();
            }
        }
        
    }
}