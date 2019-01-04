using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Rebalanser.Core;
using Rebalanser.Core.Logging;

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

        private TimeSpan onStartTime;
        private TimeSpan onStopTime;
        private bool randomiseTimes;
        private Random rand;
        
        public TestClient(ResourceMonitor resourceMonitor,
            string resourceGroup, 
            ClientOptions clientOptions,
            TimeSpan onStartTime,
            TimeSpan onStopTime,
            bool randomiseTimes)
        {
            ResourceGroup = resourceGroup;
            ClientOptions = clientOptions;
            Monitor = resourceMonitor;
            Resources = new List<string>();

            this.onStartTime = onStartTime;
            this.onStopTime = onStopTime;
            this.randomiseTimes = randomiseTimes;
            this.rand = new Random(Guid.NewGuid().GetHashCode());
        }

        public async Task StartAsync(ILogger logger)
        {
            CreateNewClient(logger);
            await Client.StartAsync(ResourceGroup, ClientOptions);
            Started = true;
        }

        public async Task StopAsync()
        {
            await Client.StopAsync(TimeSpan.FromSeconds(30));
            Started = false;
        }

        public async Task PerformActionAsync(ILogger logger)
        {
            if (Started)
            {
                logger.Info("TEST RUNNER", "Stopping client");
                Monitor.RegisterRemoveClient(this.Id);
                await StopAsync();
                logger.Info("TEST RUNNER", "Stopped client");
            }
            else
            {
                logger.Info("TEST RUNNER", "Starting client");
                await StartAsync(logger);
                logger.Info("TEST RUNNER", "Started client");
            }
        }

        private void CreateNewClient(ILogger logger)
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

                if (this.onStartTime > TimeSpan.Zero)
                {
                    if (randomiseTimes)
                    {
                        var waitTime = this.onStartTime.TotalMilliseconds * this.rand.NextDouble();
                        Thread.Sleep((int)waitTime);
                    }
                    else
                    {
                        Thread.Sleep(this.onStartTime);
                    }
                }
            };

            Client.OnUnassignment += (sender, args) =>
            {
                foreach (var resource in Resources)
                    Monitor.ReleaseResource(resource, Id);

                Resources.Clear();
                
                if (this.onStopTime > TimeSpan.Zero)
                {
                    if (randomiseTimes)
                    {
                        var waitTime = this.onStopTime.TotalMilliseconds * this.rand.NextDouble();
                        Thread.Sleep((int)waitTime);
                    }
                    else
                    {
                        Thread.Sleep(this.onStopTime);
                    }
                }
            };

            Client.OnAborted += (sender, args) =>
            {
                logger.Info("CLIENT", $"CLIENT ABORTED: {args.AbortReason}");
            };
        }
    }
}