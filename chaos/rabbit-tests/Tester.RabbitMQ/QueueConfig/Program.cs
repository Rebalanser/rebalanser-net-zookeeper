using System;
using System.Threading.Tasks;

namespace QueueConfig
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            //            var queuePrefix = args[0];
//            var queueCount = int.Parse(args[1]);
            
            var resourceGroup = "group";
            var queuePrefix = "rb";
            var queueCount = 10;
            var exchange = "input-seq";
            var rmqHost = "localhost";
            var zooKeeperHosts = "zk1:2181,zk2:2181,zk3:2181";
            var rmqNodes = "rabbit@rabbitmq1,rabbit@rabbitmq2,rabbit@rabbitmq3";
            
            var rabbitConn = new RabbitConnection()
            {
                Host = rmqHost,
                Password = "guest",
                ManagementPort = 15672,
                Port = 5672,
                Username = "guest",
                VirtualHost = "/"
            };
            var queueManager = new QueueManager();
            await queueManager.InitializeAsync(rabbitConn, zooKeeperHosts, rmqNodes);
            await queueManager.MeetDesiredStateAsync(resourceGroup, queuePrefix, queueCount, exchange);
        }
    }
}