using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Loader;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Rebalanser.Core;
using Rebalanser.ZooKeeper;
using Tester.RabbitMQ.Setup;

namespace Tester.RabbitMQ
{
    class Program
    {
        private static List<ConsumerTask> ConsumerTasks;
        private static string ZooKeeperHosts;
        private static TimeSpan MinRebalanceInterval;
        private static TimeSpan SessionTimeout;
        private static RebalancingMode RebalanceMode;

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }
        
        static async Task MainAsync(string[] args)
        {
            // support graceful shutdown in Docker
            var ended = new ManualResetEventSlim();
            var starting = new ManualResetEventSlim();

            AssemblyLoadContext.Default.Unloading += ctx =>
            {
                System.Console.WriteLine("Unloading fired");
                starting.Set();
                System.Console.WriteLine("Waiting for completion");
                ended.Wait();
            };
            
            try
            {
                var cts = new CancellationTokenSource();
                var builder = new ConfigurationBuilder()
                        .AddEnvironmentVariables("Tester.RabbitMQ.")
                        .AddCommandLine(args);
                IConfigurationRoot configuration = builder.Build();
                Task task = null;
                bool shutdown = false;

                string mode = GetMandatoryArg(configuration, "Mode");
                string rabbitMQHost = GetMandatoryArg(configuration, "RabbitMQHost");
                
                if (mode == "publish")
                {
                    LogInfo("Publish mode");
                    Console.Title = "Rebalanser Tester - RabbitMQ Publisher";
                    var exchange = GetMandatoryArg(configuration, "Exchange");
                    var stateCount = int.Parse(GetMandatoryArg(configuration, "Keys"));
                    var sendInterval = TimeSpan.FromMilliseconds(int.Parse(GetMandatoryArg(configuration, "SendIntervalMs")));

                    task = Task.Run(async () => await PublishSequenceAsync(rabbitMQHost, exchange, stateCount, sendInterval, cts.Token));
                }
                else if (mode == "consume")
                {
                    LogInfo("Consume mode");
                    Console.Title = "Rebalanser Tester - RabbitMQ Consumer";
                    string consumerGroup = GetMandatoryArg(configuration, "Group");
                    string outputQueue = GetMandatoryArg(configuration, "OutQueue");
                    ZooKeeperHosts = GetMandatoryArg(configuration, "ZooKeeperHosts");
                    RebalanceMode = GetMandatoryArg(configuration, "RebalancingMode") == "resource-barrier" ? RebalancingMode.ResourceBarrier : RebalancingMode.GlobalBarrier;
                    MinRebalanceInterval = TimeSpan.FromSeconds(int.Parse(GetMandatoryArg(configuration, "MinRebalanceIntervalSeconds")));
                    SessionTimeout = TimeSpan.FromSeconds(int.Parse(GetMandatoryArg(configuration, "RebalanserSessionTimeoutSeconds")));
                    
                    task = Task.Run(async () => await RunRebalanserAsync(rabbitMQHost, consumerGroup, outputQueue, cts.Token));
                    
                }
                else if (mode == "verify-output-seq")
                {
                    LogInfo("Output mode");
                    Console.Title = "Rebalanser Tester - Sequence Validator";
                    var queue = GetMandatoryArg(configuration, "Queue");
                    task = Task.Run(() => StartConsumingAndPrinting(rabbitMQHost, queue, cts.Token));
                }
                else if (mode == "setup")
                {
                    string outputQueue = GetMandatoryArg(configuration, "OutQueue");
                    string consumerGroup = GetMandatoryArg(configuration, "Group");
                    string queuePrefix = GetMandatoryArg(configuration, "QueuePrefix");
                    int queueCount = int.Parse(GetMandatoryArg(configuration, "QueueCount"));
                    string exchange = GetMandatoryArg(configuration, "Exchange");
                    string zooKeeperHosts = GetMandatoryArg(configuration, "ZooKeeperHosts");
                    string rmqNodes = GetMandatoryArg(configuration, "RabbitMQNodes");
                    
                    var rabbitConn = new RabbitConnection()
                    {
                        Host = rabbitMQHost,
                        Password = "guest",
                        ManagementPort = 15672,
                        Port = 5672,
                        Username = "guest",
                        VirtualHost = "/"
                    };
                    var queueManager = new QueueManager();
                    await queueManager.InitializeAsync(rabbitConn, zooKeeperHosts, rmqNodes);
                    await queueManager.MeetDesiredStateAsync(consumerGroup, queuePrefix, queueCount, exchange, outputQueue);

                    shutdown = true;
                }
                else
                {
                    Console.WriteLine("Unknown command");
                    shutdown = true;
                }
                
                
                // wait for shutdown signal
#if DEBUG
                Console.WriteLine("Press any key to shutdown");

                while (!shutdown)
                {
                    if (Console.KeyAvailable)
                    {
                        Console.WriteLine("Key pressed");
                        shutdown = true;
                    }
                    else if (task.IsCompleted)
                    {
                        Console.WriteLine("Task completed");
                        shutdown = true;
                    }

                    Thread.Sleep(500);
                }
#else
                while (!shutdown)
                {
                    if (starting.IsSet)
                        shutdown = true;
                    else if (task.IsCompleted)
                        shutdown = true;

                    Thread.Sleep(500);
                }
#endif

                Console.WriteLine("Received signal gracefully shutting down");
                cts.Cancel();
                if(task != null)
                    task.GetAwaiter().GetResult();
                ended.Set();
            }
            catch(CmdArgumentException ex)
            {
                Console.WriteLine(ex.Message);
                ended.Set();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
                ended.Set();
            }
        }

        static string GetMandatoryArg(IConfiguration configuration, string argName)
        {
            var value = configuration[argName];
            if (string.IsNullOrEmpty(value))
                throw new CmdArgumentException($"No argument {argName}");

            return value;
        }

        private static async Task PublishSequenceAsync(string rabbitMQHost, string exchange, int stateCount, TimeSpan sendInterval, CancellationToken token)
        {
            var states = new string[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y" };
            var stateIndex = 0;
            var values = new Dictionary<string, int>();
            var sendCount = 0;

            try
            {
                var factory = new ConnectionFactory() { HostName = rabbitMQHost };
                var connection = factory.CreateConnection();
                var channel = connection.CreateModel();

                try
                {
                    while(!token.IsCancellationRequested)
                    {
                        int stateSuffix = stateIndex / 25;
                        var key = $"{states[stateIndex%25]}{stateSuffix}";
                        int value = 1;
                        if (values.ContainsKey(key))
                        {
                            value = values[key];
                            values[key] = value + 1;
                        }
                        else
                        {
                            values.Add(key, 2);
                        }
                        var message = $"{key}={value}";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchange, stateIndex.ToString(), null, body);

                        stateIndex++;
                        if (stateIndex == stateCount)
                            stateIndex = 0;
                        
                        sendCount++;

                        await Task.Delay(sendInterval);
                    }
                    
                    if(token.IsCancellationRequested)
                        LogInfo("Publisher cancelled");
                }
                finally
                {
                    channel.Close();
                    connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                LogError(ex.ToString());
            }

            LogInfo("Publishing complete");
        }

        private static async Task RunRebalanserAsync(string rabbitMQHost, string consumerGroup, string outputQueue, CancellationToken token)
        {
            Providers.Register(GetProvider);
            ConsumerTasks = new List<ConsumerTask>();

            while (!token.IsCancellationRequested)
            {
                using (var client = new RebalanserClient())
                {
                    client.OnAssignment += (sender, args) =>
                    {
                        foreach (var queue in args.Resources)
                            StartConsumingAndPublishing(rabbitMQHost, queue, outputQueue);
                    };

                    client.OnUnassignment += (sender, args) =>
                    {
                        StopAllConsumptionAsync().GetAwaiter().GetResult();
                    };

                    client.OnAborted += (sender, args) =>
                    {
                        if (args.Exception != null)
                            LogInfo($"Error: {args.AbortReason} Exception: {args.Exception}");
                        else
                            LogInfo($"Error: {args.AbortReason}");
                    };

                    var clientOptions = new ClientOptions()
                    {
                        AutoRecoveryOnError = true, 
                        RestartDelay = TimeSpan.FromSeconds(30),
                        OnAssignmentDelay = TimeSpan.FromSeconds(30) // ensure this is longer than the keep alive timeout
                    };
                    await client.StartAsync(consumerGroup, clientOptions);
                    await client.BlockAsync(token, TimeSpan.FromSeconds(30));
                }

                if (!token.IsCancellationRequested)
                {
                    LogInfo("The Rebalanser client has aborted. Creating new client in 30 seconds");
                    await Task.Delay(TimeSpan.FromSeconds(30));
                }
            }
            
            if (token.IsCancellationRequested)
                LogInfo("The Rebalanser client has been cancelled and shutdown");
        }

        private static IRebalanserProvider GetProvider()
        {
            return new ZooKeeperProvider(ZooKeeperHosts,
                "/rebalanser",
                SessionTimeout,
                TimeSpan.FromSeconds(30),
                MinRebalanceInterval,
                RebalanceMode,
                new TestLogger());
        }
        
        private static void StartConsumingAndPublishing(string rabbitMQHost, string queueName, string outputQueue)
        {
            var cts = new CancellationTokenSource();
            
            var task = Task.Factory.StartNew(() =>
            {
                try
                {
                    var factory = new ConnectionFactory() { HostName = rabbitMQHost, RequestedHeartbeat = 10 };
                    var connection = factory.CreateConnection();
                    var receiveChannel = connection.CreateModel();
                    var sendChannel = connection.CreateModel();
                    sendChannel.ConfirmSelect();
                    try
                    {
                        receiveChannel.BasicQos(0, 1, false);
                        var consumer = new EventingBasicConsumer(receiveChannel);
                        consumer.Received += (model, ea) =>
                        {
                            var body = ea.Body;
                            var message = Encoding.UTF8.GetString(body);
                            sendChannel.BasicPublish(exchange: "",
                                     routingKey: outputQueue,
                                     basicProperties: null,
                                     body: body);
                            sendChannel.WaitForConfirms(TimeSpan.FromSeconds(10));
                            receiveChannel.BasicAck(ea.DeliveryTag, false);
                            //Console.WriteLine(message);
                        };

                        receiveChannel.BasicConsume(queue: queueName,
                                             autoAck: false,
                                             consumer: consumer);
                        
                        LogInfo("Consumer started for queue: " + queueName);

                        while (!cts.Token.IsCancellationRequested)
                            Thread.Sleep(100);
                    }
                    finally
                    {
                        receiveChannel.Close();
                        sendChannel.Close();
                        connection.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    LogError(ex.ToString());
                }

                if (cts.Token.IsCancellationRequested)
                {
                    //LogInfo("Cancellation signal received for " + queueName);
                }
                else
                    LogInfo("Consumer stopped for " + queueName);
            }, TaskCreationOptions.LongRunning);

            ConsumerTasks.Add(new ConsumerTask() { Cts = cts, Consumer = task });
        }

        private static async Task StopAllConsumptionAsync()
        {
            foreach (var ct in ConsumerTasks)
                ct.Cts.Cancel();

            await Task.WhenAll(ConsumerTasks.Select(x => x.Consumer));
        }

        private static void StartConsumingAndPrinting(string rabbitMQHost, string queueName, CancellationToken token)
        {
            try
            {
                var factory = new ConnectionFactory() { HostName = rabbitMQHost };
                var connection = factory.CreateConnection();
                var receiveChannel = connection.CreateModel();
                int receiveCount = 0;

                var states = new Dictionary<string, int>();
                try
                {
                    var consumer = new EventingBasicConsumer(receiveChannel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var parts = message.Split("=");
                        var key = parts[0];
                        var currValue = int.Parse(parts[1]);
                        
                        if (states.ContainsKey(key))
                        {
                            var lastValue = states[key];

                            if (lastValue + 1 < currValue)
                            {
                                Console.WriteLine($"{message} JUMP FORWARDS {currValue - lastValue}");
                            }
                            else if (currValue < lastValue)
                            {
                                Console.WriteLine($"{message} JUMP BACKWARDS {lastValue - currValue}");
                            }
                            else if (currValue == lastValue)
                            {
                                Console.WriteLine($"{message} DUPLICATE");
                            }
                            else
                            {
                                receiveCount++;
                                if(receiveCount % 10000 == 0)
                                    Console.WriteLine(message + " Received: " + receiveCount);
                            }

                            states[key] = currValue;
                        }
                        else
                        {
                            if(currValue > 1)
                                Console.WriteLine($"{message} JUMP FORWARDS {currValue}");
                            else
                                Console.WriteLine(message);

                            states.Add(key, currValue);
                        }

                        receiveChannel.BasicAck(ea.DeliveryTag, false);
                    };

                    receiveChannel.BasicConsume(queue: queueName,
                                         autoAck: false,
                                         consumer: consumer);

                    while (!token.IsCancellationRequested)
                        Thread.Sleep(100);
                }
                finally
                {
                    receiveChannel.Close();
                    connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                LogError(ex.ToString());
            }

            if (token.IsCancellationRequested)
                LogInfo("Cancellation signal received for " + queueName);
            else
                LogInfo("Consumer stopped for " + queueName);
        }

        private static void LogInfo(string text)
        {
            Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: INFO  : {text}");
        }

        private static void LogError(string text)
        {
            Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR  : {text}");
        }
    }

    internal class CmdArgumentException : Exception
    {
        public CmdArgumentException(string message)
        : base(message)
        {
            
        }
    }
}