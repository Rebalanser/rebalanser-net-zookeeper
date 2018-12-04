using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebalanser.Core;
using Rebalanser.Core.Logging;
using Rebalanser.ZooKeeper;

namespace ZkTester
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
#if DEBUG
                Console.WriteLine("Press enter to start");
                Console.ReadLine();
#endif

                var p = new Program();
                p.RunMultipleClientsAsync().Wait();
            }
            else
            {
                var p = new Program();
                p.RunSingleClientAsync().Wait();
            }
        }

        private List<ClientTask> clientTasks;

        async Task RunSingleClientAsync()
        {
            var cts = new CancellationTokenSource();
            Task t = Task.Run(async () => { await StartClientAsync("1", cts.Token); });
            
            Console.WriteLine("Press any key to shutdown client");
            Console.ReadKey();
            Console.WriteLine("Shuting down client");
            cts.Cancel();
            await t;
            Console.WriteLine("Client stopped");
        }
        
        async Task RunMultipleClientsAsync()
        {
            int id = 0;
            clientTasks = new List<ClientTask>();
            
            while (true)
            {
                Console.WriteLine($"{clientTasks.Count} nodes");
                Console.WriteLine("Type \"+\" to add, \"c\" to kill the coordinator, \"f\" to kill a follower");
                var input = Console.ReadKey();
                
                if (input.Key == ConsoleKey.Add)
                {
                    id++;
                    var cts = new CancellationTokenSource();
                    Task t = Task.Run(async () => { await StartClientAsync(id.ToString(), cts.Token); });
                    clientTasks.Add(new ClientTask()
                    {
                        Cts = cts,
                        Client = t,
                        Id = id.ToString()
                    }); 
                }
                else if(input.KeyChar == 'c')
                {
                    if (clientTasks.Count > 0)
                    {
                        var index = 0;
                        Console.WriteLine($"Stopping coordinator with client number {clientTasks[index].Id}");
                        clientTasks[index].Cts.Cancel();
                        await clientTasks[index].Client;
                        Console.WriteLine($"Stopped coordinator {clientTasks[index].Id}");
                        clientTasks.RemoveAt(index);
                    }
                    else
                    {
                        Console.WriteLine("There are no clients to kill");
                    }
                }
                else if(input.KeyChar == 'f')
                {
                    if (clientTasks.Count > 1)
                    {
                        var index = 1;
                        Console.WriteLine($"Stopping follower with client number {clientTasks[index].Id}");
                        clientTasks[index].Cts.Cancel();
                        await clientTasks[index].Client;
                        Console.WriteLine($"Stopped follower {clientTasks[index].Id}");
                        clientTasks.RemoveAt(index);
                    }
                    else
                    {
                        Console.WriteLine("There are no clients to kill");
                    }
                }
            }

            Console.ReadLine();
        }

        private async Task StartClientAsync(string id, CancellationToken token)
        {
            Console.WriteLine($"{id}: Starting");
            var zkProvider = new ZooKeeperProvider("localhost:2181", 
                "/rebalanser", 
                TimeSpan.FromMinutes(5),
                RebalancingMode.GlobalBarrier,
                new ConsoleLogger());
            Providers.Register(zkProvider);
            Random r = new Random(Guid.NewGuid().GetHashCode());
            
            using (var context = new RebalanserContext())
            {
                context.OnAssignment += (sender, args) =>
                {
                    Thread.Sleep(r.Next(5000));
                    var resources = context.GetAssignedResources();
                    if(resources.Any())
                        Console.WriteLine($"{id}: Resources Assigned: {string.Join(",", resources)}");
                    else
                        Console.WriteLine($"{id}: No resources Assigned");
                };

                context.OnCancelAssignment += (sender, args) =>
                {
                    Thread.Sleep(r.Next(5000));
                    Console.WriteLine($"{id}: Consumer subscription cancelled");
                };

                context.OnError += (sender, args) =>
                {
                    Console.WriteLine($"{id}: Error: {args.Message}, automatic recovery set to: {args.AutoRecoveryEnabled}, Exception: {args.Exception.Message}");
                };

                await context.StartAsync("mygroup", new ContextOptions() { AutoRecoveryOnError = true, RestartDelay = TimeSpan.FromSeconds(30) });

                Console.WriteLine($"{id}: Started");
                while (!token.IsCancellationRequested)
                    await Task.Delay(100);

                Console.WriteLine($"{id}: Cancelled");
            }
        }
    }
}