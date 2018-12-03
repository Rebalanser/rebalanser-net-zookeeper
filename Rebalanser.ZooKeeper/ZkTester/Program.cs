using System;
using System.Collections.Generic;
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
#if DEBUG
            Console.WriteLine("Press enter to start");
            Console.ReadLine();
#endif
            
            var p = new Program();
            p.MainAsync().Wait();
        }

        private List<ClientTask> clientTasks;
        
        async Task MainAsync()
        {
            int id = 0;
            clientTasks = new List<ClientTask>();
            
            while (true)
            {
                Console.WriteLine($"{clientTasks.Count} nodes");
                Console.WriteLine("Type \"+\" to add, or a number to remove a client (at that index)");
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
                else
                {
                    int index = int.Parse(input.KeyChar.ToString());
                    if (index > clientTasks.Count)
                    {
                        Console.WriteLine("Index too big");
                    }
                    else
                    {
                        Console.WriteLine($"Stopping client {clientTasks[index].Id}");
                        clientTasks[index].Cts.Cancel();
                        await clientTasks[index].Client;
                        Console.WriteLine($"Stopped client {clientTasks[index].Id}");
                        clientTasks.RemoveAt(index);
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
            
            using (var context = new RebalanserContext())
            {
                context.OnAssignment += (sender, args) =>
                {
                    var resources = context.GetAssignedResources();
                    Console.WriteLine($"{id}: RESOURCES ASSIGNED: {string.Join(",", resources)}");
                };

                context.OnCancelAssignment += (sender, args) =>
                {
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