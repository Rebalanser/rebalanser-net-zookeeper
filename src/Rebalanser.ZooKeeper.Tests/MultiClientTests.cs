using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper.Tests.Helpers;
using Xunit;

namespace Rebalanser.ZooKeeper.Tests
{
    public class MultiClientTests : IDisposable
    {
        private ZkHelper zkHelper;

        public MultiClientTests()
        {
            this.zkHelper = new ZkHelper();
        }


        [Fact]
        public async Task ResourceBarrier_GivenSixResourcesAndThreeClients_ThenEachClientGetsTwoResources()
        {
            Providers.Register(GetResourceBarrierProvider);
            await GivenSixResourcesAndThreeClients_ThenEachClientGetsTwoResources();
        }
        
        [Fact]
        public async Task GlobalBarrier_GivenSixResourcesAndThreeClients_ThenEachClientGetsTwoResources()
        {
            Providers.Register(GetGlobalBarrierProvider);
            await GivenSixResourcesAndThreeClients_ThenEachClientGetsTwoResources();
        }
        
        private async Task GivenSixResourcesAndThreeClients_ThenEachClientGetsTwoResources()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("/rebalanser", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 6);

            // ACT
            var (client1, testEvents1) = CreateClient();
            var (client2, testEvents2) = CreateClient();
            var (client3, testEvents3) = CreateClient();

            await client1.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            await client2.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            await client3.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});

            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // ASSERT
            // check client 1
            AssertAssigned(testEvents1, 2);
            AssertAssigned(testEvents2, 2);
            AssertAssigned(testEvents3, 2);
            
            await client1.StopAsync();
            await client2.StopAsync();
            await client3.StopAsync();
        }

        [Fact]
        public async Task ResourceBarrier_GivenMultipleResourcesAndThreeClientsAtStartWithClientsStoppingOneByOne_ThenResourceAssignmentsReassignedAccordingly()
        {
            Providers.Register(GetResourceBarrierProvider);
            await GivenMultipleResourcesAndThreeClientsAtStartWithClientsStoppingOneByOne_ThenResourceAssignmentsReassignedAccordingly();
        }
        
        [Fact]
        public async Task GlobalBarrier_GivenMultipleResourcesAndThreeClientsAtStartWithClientsStoppingOneByOne_ThenResourceAssignmentsReassignedAccordingly()
        {
            Providers.Register(GetGlobalBarrierProvider);
            await GivenMultipleResourcesAndThreeClientsAtStartWithClientsStoppingOneByOne_ThenResourceAssignmentsReassignedAccordingly();
        }
        
        private async Task GivenMultipleResourcesAndThreeClientsAtStartWithClientsStoppingOneByOne_ThenResourceAssignmentsReassignedAccordingly()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("/rebalanser", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 6);

            // ACT - start up three clients
            var (client1, testEvents1) = CreateClient();
            var (client2, testEvents2) = CreateClient();
            var (client3, testEvents3) = CreateClient();

            await client1.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            await client2.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            await client3.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});

            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // ASSERT all clients have been assigned equal resources
            AssertAssigned(testEvents1, 2);
            AssertAssigned(testEvents2, 2);
            AssertAssigned(testEvents3, 2);
            
            // ACT - stop one client
            await client1.StopAsync();
            
            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // ASSERT - stopped client has had resources unassigned and remaining two
            // have been equally assigned the 6 resources between them
            AssertUnassignedOnly(testEvents1);
            AssertUnassignedThenAssigned(testEvents2, 3);
            AssertUnassignedThenAssigned(testEvents3, 3);
            
            // ACT - stop one client
            await client2.StopAsync();
            
            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // ASSERT - stopped client has had resources unassigned and remaining one client
            // have been assigned all 6 resources between them
            AssertNoEvents(testEvents1);
            AssertUnassignedOnly(testEvents2);
            AssertUnassignedThenAssigned(testEvents3, 6);
            
            // clean up
            await client3.StopAsync();
        }

        [Fact]
        public async Task ResourceBarrier_GivenMultipleResourcesAndOneClientAtStartWithNewClientsStartingOneByOne_ThenResourceAssignmentsReassignedAccordingly()
        {
            Providers.Register(GetResourceBarrierProvider);
            await GivenMultipleResourcesAndOneClientAtStartWithNewClientsStartingOneByOne_ThenResourceAssignmentsReassignedAccordingly();
        }
        
        [Fact]
        public async Task GlobalBarrier_GivenMultipleResourcesAndOneClientAtStartWithNewClientsStartingOneByOne_ThenResourceAssignmentsReassignedAccordingly()
        {
            Providers.Register(GetGlobalBarrierProvider);
            await GivenMultipleResourcesAndOneClientAtStartWithNewClientsStartingOneByOne_ThenResourceAssignmentsReassignedAccordingly();
        }
        
        private async Task GivenMultipleResourcesAndOneClientAtStartWithNewClientsStartingOneByOne_ThenResourceAssignmentsReassignedAccordingly()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("/rebalanser", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 6);

            // ACT - create three clients and start one
            var (client1, testEvents1) = CreateClient();
            var (client2, testEvents2) = CreateClient();
            var (client3, testEvents3) = CreateClient();

            await client1.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // ASSERT - client 1 that has started has been assigned all resources
            // but that clients 2 and 3 that have not started has been assigned nothing
            AssertAssigned(testEvents1, 6);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            // ACT - start one client
            await client2.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // ASSERT - client 1 and 2 that have started have been equally assigned resources
            // but that client 3 that has not started has been assigned nothing
            AssertUnassignedThenAssigned(testEvents1, 3);
            AssertAssigned(testEvents2, 3);
            AssertNoEvents(testEvents3);
            
            // ACT - start one client
            await client3.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // ASSERT - all clients have been equally assigned the resources
            AssertUnassignedThenAssigned(testEvents1, 2);
            AssertUnassignedThenAssigned(testEvents2, 2);
            AssertAssigned(testEvents3, 2);
            
            // clean up
            await client1.StopAsync();
            await client2.StopAsync();
            await client3.StopAsync();
        }

        [Fact]
        public async Task ResourceBarrier_GivenOneResourceAndOneClient_ThenAdditionalClientsDoNotTriggerRebalancing()
        {
            Providers.Register(GetResourceBarrierProvider);
            await GivenOneResourceAndOneClient_ThenAdditionalClientsDoNotTriggerRebalancing();
        }
        
        [Fact]
        public async Task GlobalBarrier_GivenOneResourceAndOneClient_ThenAdditionalClientsDoNotTriggerRebalancing()
        {
            Providers.Register(GetGlobalBarrierProvider);
            await GivenOneResourceAndOneClient_ThenAdditionalClientsDoNotTriggerRebalancing();
        }
        
        private async Task GivenOneResourceAndOneClient_ThenAdditionalClientsDoNotTriggerRebalancing()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("/rebalanser", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 1);

            // ACT - create three clients and start one
            var (client1, testEvents1) = CreateClient();
            var (client2, testEvents2) = CreateClient();
            var (client3, testEvents3) = CreateClient();

            await client1.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - client 1 that has started has been assigned all resources
            // but that clients 2 and 3 that have not started has been assigned nothing
            AssertAssigned(testEvents1, 1);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            // ACT - start one client
            await client2.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - no rebalancing occurs
            AssertNoEvents(testEvents1);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            // ACT - start one client
            await client3.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - no rebalancing occurs
            AssertNoEvents(testEvents1);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            // ACT - stop one client
            await client2.StopAsync();
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - no rebalancing occurs
            AssertNoEvents(testEvents1);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            // clean up
            await client1.StopAsync();
            await client3.StopAsync();
        }

        [Fact]
        public async Task ResourceBarrier_GivenTwoResourcesAndThreeClients_ThenRemovingUnassignedClientDoesNotTriggerRebalancing()
        {
            Providers.Register(GetResourceBarrierProvider);
            await GivenTwoResourcesAndThreeClients_ThenRemovingUnassignedClientDoesNotTriggerRebalancing();
        }
        
        [Fact]
        public async Task GlobalBarrier_GivenTwoResourcesAndThreeClients_ThenRemovingUnassignedClientDoesNotTriggerRebalancing()
        {
            Providers.Register(GetGlobalBarrierProvider);
            await GivenTwoResourcesAndThreeClients_ThenRemovingUnassignedClientDoesNotTriggerRebalancing();
        }
        
        private async Task GivenTwoResourcesAndThreeClients_ThenRemovingUnassignedClientDoesNotTriggerRebalancing()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("/rebalanser", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 2);

            // ACT - create three clients and start one
            var (client1, testEvents1) = CreateClient();
            var (client2, testEvents2) = CreateClient();
            var (client3, testEvents3) = CreateClient();

            await client1.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - client 1 has started has each been assigned both resources
            // but that clients 2 and 3 that have not started have been assigned nothing
            AssertAssigned(testEvents1, 2);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            await client2.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - client 1 and 2 have started have each been assigned a resource
            // but that clients 3 that has not started has been assigned nothing
            AssertUnassignedThenAssigned(testEvents1, 1);
            AssertAssigned(testEvents2, 1);
            AssertNoEvents(testEvents3);
            
            // ACT - start client 3
            await client3.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - no rebalancing occurs
            AssertNoEvents(testEvents1);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            // ACT - remove client3
            await client3.StopAsync();
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - no rebalancing occurs
            AssertNoEvents(testEvents1);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            // clean up
            await client1.StopAsync();
            await client2.StopAsync();
        }

        [Fact]
        public async Task ResourceBarrier_GivenTwoResourcesAndThreeClients_ThenRemovingAssignedClientDoesTriggerRebalancing()
        {
            Providers.Register(GetResourceBarrierProvider);
            await GivenTwoResourcesAndThreeClients_ThenRemovingAssignedClientDoesTriggerRebalancing();
        }
        
        [Fact]
        public async Task GlobalBarrier_GivenTwoResourcesAndThreeClients_ThenRemovingAssignedClientDoesTriggerRebalancing()
        {
            Providers.Register(GetGlobalBarrierProvider);
            await GivenTwoResourcesAndThreeClients_ThenRemovingAssignedClientDoesTriggerRebalancing();
        }
        
        private async Task GivenTwoResourcesAndThreeClients_ThenRemovingAssignedClientDoesTriggerRebalancing()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("/rebalanser", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 2);

            // ACT - create three clients and start one
            var (client1, testEvents1) = CreateClient();
            var (client2, testEvents2) = CreateClient();
            var (client3, testEvents3) = CreateClient();

            await client1.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - client 1 has started has each been assigned both resources
            // but that clients 2 and 3 that have not started have been assigned nothing
            AssertAssigned(testEvents1, 2);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            await client2.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - client 1 and 2 have started have each been assigned a resource
            // but that clients 3 that has not started has been assigned nothing
            AssertUnassignedThenAssigned(testEvents1, 1);
            AssertAssigned(testEvents2, 1);
            AssertNoEvents(testEvents3);
            
            // ACT - start client 3
            await client3.StartAsync(groupName, new ClientOptions() {AutoRecoveryOnError = false});
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - no rebalancing occurs
            AssertNoEvents(testEvents1);
            AssertNoEvents(testEvents2);
            AssertNoEvents(testEvents3);
            
            // ACT - remove client2
            await client2.StopAsync();
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // ASSERT - rebalancing occurs
            AssertUnassignedThenAssigned(testEvents1, 1);
            AssertUnassignedOnly(testEvents2);
            AssertAssigned(testEvents3, 1);
            
            // clean up
            await client1.StopAsync();
            await client3.StopAsync();
        }

        private void AssertAssigned(List<TestEvent> testEvents, int count)
        {
            Assert.Equal(1, testEvents.Count);
            Assert.Equal(EventType.Assignment, testEvents[0].EventType);
            Assert.Equal(count, testEvents[0].Resources.Count);
            testEvents.Clear();
        }
        
        private void AssertUnassignedThenAssigned(List<TestEvent> testEvents, int count)
        {
            Assert.Equal(2, testEvents.Count);
            Assert.Equal(EventType.Unassignment, testEvents[0].EventType);
            Assert.Equal(EventType.Assignment, testEvents[1].EventType);
            Assert.Equal(count, testEvents[1].Resources.Count);
            testEvents.Clear();
        }
        
        private void AssertUnassignedOnly(List<TestEvent> testEvents)
        {
            Assert.Equal(1, testEvents.Count);
            Assert.Equal(EventType.Unassignment, testEvents[0].EventType);
            testEvents.Clear();
        }
        
        private void AssertNoEvents(List<TestEvent> testEvents)
        {
            Assert.Equal(0, testEvents.Count);
            testEvents.Clear();
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

            client1.OnAborted += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Error
                });
                Console.WriteLine($"OnAborted: {args.AbortReason} {args.Exception.ToString()}");
            };

            return (client1, testEvents);
        }

        private IRebalanserProvider GetResourceBarrierProvider()
        {
            return new ZooKeeperProvider(ZkHelper.ZooKeeperHosts, 
                "/rebalanser", 
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(5),
                RebalancingMode.ResourceBarrier,
                new TestOutputLogger());
        }
        
        private IRebalanserProvider GetGlobalBarrierProvider()
        {
            return new ZooKeeperProvider(ZkHelper.ZooKeeperHosts, 
                "/rebalanser", 
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(5),
                RebalancingMode.GlobalBarrier,
                new TestOutputLogger());
        }

        public void Dispose()
        {
            if (this.zkHelper != null)
                Task.Run(async () => await this.zkHelper.CloseAsync()).Wait();
        }
    }
}