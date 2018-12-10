using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.Tests.Helpers;
using Xunit;

namespace Rebalanser.ZooKeeper.Tests
{
    public class SingleClientTests : IDisposable
    {
        private ZkHelper zkHelper;

        public SingleClientTests()
        {
            this.zkHelper = new ZkHelper();
        }
        
        [Fact]
        public async Task GivenSingleClient_ThenGetsAllResourcesAssigned()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("localhost:2181", "/rebalanser", TimeSpan.FromSeconds(100));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 5);

            RegisterZookeeperProvider();
            var expectedAssignedResources = new List<string>() {"res0", "res1", "res2", "res3", "res4"};
                
            
            // ACT
            var actualAssignedResources = new List<string>();
            var assigned = false;
            using (var context = new RebalanserContext())
            {
                context.OnAssignment += (sender, args) =>
                {
                    actualAssignedResources = context.GetAssignedResources().ToList();
                    assigned = true;
                };

                context.OnCancelAssignment += (sender, args) =>
                {
                    
                };

                context.OnError += (sender, args) =>
                {
                    
                };

                await context.StartAsync(groupName, new ContextOptions() { AutoRecoveryOnError = false });

                var sw = new Stopwatch();
                sw.Start();
                while (!assigned && sw.Elapsed < TimeSpan.FromSeconds(30))
                    await Task.Delay(100);
            }

            // ASSERT
            Assert.True(assigned, "Resources not assigned");
            Assert.True(ResourcesMatch(expectedAssignedResources, actualAssignedResources), "Expected resources do not match actual resources assigned");
        }

        private bool ResourcesMatch(List<string> expectedRes, List<string> actualRes)
        {
            return expectedRes.OrderBy(x => x).SequenceEqual(actualRes.OrderBy(x => x));
        }

        private void RegisterZookeeperProvider()
        {
            var zkProvider = new ZooKeeperProvider("localhost:2181", 
                "/rebalanser", 
                TimeSpan.FromSeconds(100),
                RebalancingMode.ResourceBarrier,
                new ConsoleLogger());
            Providers.Register(zkProvider);
        }

        public void Dispose()
        {
            if (this.zkHelper != null)
                Task.Run(async () => await this.zkHelper.CloseAsync()).Wait();
        }
    }
}