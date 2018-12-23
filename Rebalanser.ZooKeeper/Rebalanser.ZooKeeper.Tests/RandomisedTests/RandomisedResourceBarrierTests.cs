using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.Tests.Helpers;
using Rebalanser.ZooKeeper.Tests.RandomisedTests.TestComponents;
using Xunit;

namespace Rebalanser.ZooKeeper.Tests.RandomisedTests
{
    
    public class RandomisedResourceBarrierTests : IDisposable
    {
        private ZkHelper zkHelper;

        public RandomisedResourceBarrierTests()
        {
            this.zkHelper = new ZkHelper();
        }

        [Fact]
        public async Task RandomisedReleaseBarrierTest_ThreeClientsSixResources()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("localhost:2181", "/rebalanser", TimeSpan.FromSeconds(20));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 6);
            
            var resourceMonitor = new ResourceMonitor();
            List<int> resSuffixes = new List<int>();
            for (int i = 0; i < 6; i++)
            {
                resourceMonitor.CreateResource($"res{i}");
                resSuffixes.Add(i);
            }
             
            Providers.Register(GetProvider);
            

            var clientOptions = new ClientOptions()
            {
                AutoRecoveryOnError = true, 
                RestartDelay = TimeSpan.FromSeconds(10)
            };
            
            var clients = new List<TestClient>();
            clients.Add(new TestClient(resourceMonitor, groupName, clientOptions));
            clients.Add(new TestClient(resourceMonitor, groupName, clientOptions));
            clients.Add(new TestClient(resourceMonitor, groupName, clientOptions));
            
            await clients[0].StartAsync();
            await clients[1].StartAsync();
            await clients[2].StartAsync();
            
            // ACT
            var sw = new Stopwatch();
            sw.Start();
            var rand = new Random(Guid.NewGuid().GetHashCode());
            while (sw.Elapsed < TimeSpan.FromMinutes(120))
            {
                var action = rand.Next(2);
                if (action == 0)
                {
                    var clientIndex = rand.Next(3);
                    await clients[clientIndex].PerformActionAsync();
                }
                else
                {
                    var resAction = rand.Next(2);
                    if (resAction == 0 && resSuffixes.Count > 1)
                    {
                        var resSuffix = resSuffixes.Max() + 1;
                        resSuffixes.Add(resSuffix);
                        resourceMonitor.AddResource($"res{resSuffix}");
                        await this.zkHelper.AddResourceAsync(groupName, $"res{resSuffix}");
                    }
                    else
                    {
                        var index = rand.Next(resSuffixes.Count);
                        var resSuffix = resSuffixes[index];
                        resSuffixes.RemoveAt(index);
                        resourceMonitor.RemoveResource($"res{resSuffix}");
                        await this.zkHelper.DeleteResourceAsync(groupName, $"res{resSuffix}");
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(60));
                resourceMonitor.PrintEvents($"/home/jack/tmp/rebalanser-zk/test-{groupName}");
                Assert.False(resourceMonitor.ViolationsExist());
                if(clients.Any(x => x.Started))
                    Assert.True(resourceMonitor.AllResourcesAssigned());
            }
            
            // ASSERT
            Assert.True(true); // if we got here then no errors occurred
            await clients[0].StopAsync();
            await clients[1].StopAsync();
            await clients[2].StopAsync();
        }
        
        [Fact]
        public async Task RandomisedReleaseBarrierTest_ThirtyClientsSixtyResources()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("localhost:2181", "/rebalanser", TimeSpan.FromSeconds(20));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 60);
            
            var resourceMonitor = new ResourceMonitor();
            List<int> resSuffixes = new List<int>();
            for (int i = 0; i < 60; i++)
            {
                resourceMonitor.CreateResource($"res{i}");
                resSuffixes.Add(i);
            }
             
            Providers.Register(GetProvider);
            

            var clientOptions = new ClientOptions()
            {
                AutoRecoveryOnError = true, 
                RestartDelay = TimeSpan.FromSeconds(10)
            };
            
            var clients = new List<TestClient>();
            for (int i = 0; i < 30; i++)
                clients.Add(new TestClient(resourceMonitor, groupName, clientOptions));
            
            for (int i = 0; i < 30; i++)
                await clients[i].StartAsync();

            await Task.Delay(TimeSpan.FromSeconds(30));
            
            // ACT
            var sw = new Stopwatch();
            sw.Start();
            var rand = new Random(Guid.NewGuid().GetHashCode());
            while (sw.Elapsed < TimeSpan.FromMinutes(120))
            {
                var action = rand.Next(2);
                if (action == 0)
                {
                    var clientIndex = rand.Next(30);
                    await clients[clientIndex].PerformActionAsync();
                }
                else
                {
                    var resAction = rand.Next(2);
                    if (resAction == 0 && resSuffixes.Count > 1)
                    {
                        var resSuffix = resSuffixes.Max() + 1;
                        resSuffixes.Add(resSuffix);
                        resourceMonitor.AddResource($"res{resSuffix}");
                        await this.zkHelper.AddResourceAsync(groupName, $"res{resSuffix}");
                    }
                    else
                    {
                        var index = rand.Next(resSuffixes.Count);
                        var resSuffix = resSuffixes[index];
                        resSuffixes.RemoveAt(index);
                        resourceMonitor.RemoveResource($"res{resSuffix}");
                        await this.zkHelper.DeleteResourceAsync(groupName, $"res{resSuffix}");
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(60));
                resourceMonitor.PrintEvents($"/home/jack/tmp/rebalanser-zk/test-{groupName}");
                Assert.False(resourceMonitor.ViolationsExist());
                if(clients.Any(x => x.Started))
                    Assert.True(resourceMonitor.AllResourcesAssigned());
            }
            
            // ASSERT
            Assert.True(true); // if we got here then no errors occurred
            await clients[0].StopAsync();
            await clients[1].StopAsync();
            await clients[2].StopAsync();
        }
        
        private IRebalanserProvider GetProvider()
        {
            return new ZooKeeperProvider("localhost:2181", 
                "/rebalanser", 
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(20),
                RebalancingMode.ResourceBarrier,
                new ConsoleLogger());
        }
        
        public void Dispose()
        {
            if (this.zkHelper != null)
                Task.Run(async () => await this.zkHelper.CloseAsync()).Wait();
        }
    }
}