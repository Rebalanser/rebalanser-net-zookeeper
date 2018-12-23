using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Rebalanser.Core;

namespace Rebalanser.ZooKeeper.Tests.RandomisedTests.TestComponents
{
    public class TestClient
    {
        public string Id { get; set; }
        public RebalanserClient Client { get; set; }
        public IList<string> Resources { get; set; }
        public bool Started { get; set; }
        public string ResourceGroup { get; set; }
        public ClientOptions ClientOptions { get; set; }
        public ResourceMonitor Monitor { get; set; }
        public static int ClientNumber;
        
        public TestClient(ResourceMonitor resourceMonitor,
            string resourceGroup, 
            ClientOptions clientOptions)
        {
            ResourceGroup = resourceGroup;
            ClientOptions = clientOptions;
            Monitor = resourceMonitor;
            Resources = new List<string>();
        }

        public async Task StartAsync()
        {
            CreateNewClient();
            await Client.StartAsync(ResourceGroup, ClientOptions);
            Started = true;
        }

        public async Task StopAsync()
        {
            await Client.StopAsync(TimeSpan.FromSeconds(30));
            Started = false;
        }

        public async Task PerformActionAsync()
        {
            if (Started)
            {
                Monitor.RegisterRemoveClient(this.Id);
                await StopAsync();
            }
            else
            {
                await StartAsync();
            }
        }

        private void CreateNewClient()
        {
            Id = $"Client{ClientNumber}";
            ClientNumber++;
            Monitor.RegisterAddClient(Id);
            Client = new RebalanserClient();
            Client.OnAssignment += (sender, args) =>
            {
                Resources = args.Resources;
                foreach(var resource in args.Resources)
                    Monitor.ClaimResource(resource, Id);
            };

            Client.OnUnassignment += (sender, args) =>
            {
                foreach (var resource in Resources)
                    Monitor.ReleaseResource(resource, Id);

                Resources.Clear();
            };

            Client.OnError += (sender, args) =>
            {
                // TODO
            };
        }
    }
}