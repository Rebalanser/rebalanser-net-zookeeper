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

            Providers.Register(GetProvider);
            var expectedAssignedResources = new List<string>() {"res0", "res1", "res2", "res3", "res4"};
            
            // ACT
            var (client, testEvents) = CreateClient();
            await client.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});

            await Task.Delay(TimeSpan.FromSeconds(150));
            
            // ASSERT
            Assert.Equal(2, testEvents.Count);
            Assert.Equal(EventType.Unassignment, testEvents[0].EventType);
            Assert.Equal(EventType.Assignment, testEvents[1].EventType);
            Assert.True(ResourcesMatch(expectedAssignedResources, testEvents[1].Resources.ToList()));
            
            await client.StopAsync(TimeSpan.FromSeconds(30));
        }
        
        private (RebalanserClient, List<TestEvent> testEvents) CreateClient()
        {
            var client1 = new RebalanserClient();
            var testEvents = new List<TestEvent>();
            client1.OnAssignment += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Assignment,
                    Resources = args.Resources 
                });
            };

            client1.OnUnassignment += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Unassignment
                });
            };

            client1.OnError += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Error
                });
                Console.WriteLine($"OnError: {args.Exception.ToString()}");
            };

            return (client1, testEvents);
        }

        private bool ResourcesMatch(List<string> expectedRes, List<string> actualRes)
        {
            return expectedRes.OrderBy(x => x).SequenceEqual(actualRes.OrderBy(x => x));
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