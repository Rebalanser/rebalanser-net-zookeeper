using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using org.apache.zookeeper;

namespace QueueConfig
{
    public class QueueManager
    {
        private HttpClient httpClient;
        private ZkHelper zkHelper;
        private List<string> nodes;
        private int currentNode;
       
        public async Task InitializeAsync(RabbitConnection rabbitConnection, string zooKeeperHosts, string rabbitMQNodes)
        {
            var sw = new Stopwatch();
            sw.Start();
            this.httpClient = new HttpClient();
            this.httpClient.BaseAddress = new Uri($"http://{rabbitConnection.Host}:{rabbitConnection.ManagementPort}/api/");
            var byteArray = Encoding.ASCII.GetBytes($"{rabbitConnection.Username}:{rabbitConnection.Password}");
            this.httpClient.DefaultRequestHeaders.Authorization
                = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));

            this.nodes = rabbitMQNodes.Split(",").ToList();
            this.zkHelper = new ZkHelper();
            await this.zkHelper.InitializeAsync(zooKeeperHosts, "/rebalanser", TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
        }
        
        public async Task MeetDesiredStateAsync(string resourceGroup, string queuePrefix, int queueCount, string exchange)
        {
            await PrepareQueuesRabbitMqAsync(queuePrefix, queueCount, exchange);
            await this.zkHelper.PrepareResourceGroupAsync(resourceGroup, queuePrefix, queueCount);
        }

        private async Task PrepareQueuesRabbitMqAsync(string queuePrefix, int queueCount, string exchange)
        {
            var response = await httpClient.GetAsync("queues");
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Could not get queues, response code {response.StatusCode}\r\n");
            }
            
            var json = await response.Content.ReadAsStringAsync();
            var queues = JArray.Parse(json);
            var queueNames = queues.Select(x => x["name"].Value<string>()).Where(x => x.StartsWith(queuePrefix)).ToList();

            if (queueNames.Count == queueCount)
            {
            }
            else if (queueNames.Count > queueCount)
            {
                int removeCount = queueNames.Count - queueCount;
                for (int i = 0; i < removeCount; i++)
                {
                    var queue = queueNames.OrderByDescending(x => x).Last();
                    await DeleteQueueRabbitMQAsync(queue);
                    queueNames.Remove(queue);
                    Console.Write($"Removed queue {queue} in RabbitMQ\r\n");
                }
            }
            else if (queueNames.Count < queueCount)
            {
                int addCount = queueCount - queueNames.Count;
                int maxSuffix = queueNames.Count;
                for (int i = 0; i < addCount; i++)
                {
                    maxSuffix++;
                    var queue = queuePrefix + "_" + maxSuffix.ToString().PadLeft(5, '0');
                    var node = nodes[currentNode];
                    currentNode++;
                    if (currentNode == this.nodes.Count)
                        currentNode = 0;
                    await PutQueueRabbitMQAsync(exchange, queue, node);
                    queueNames.Add(queue);
                    Console.Write($"Added queue {queue} in RabbitMQ\r\n");
                }
            }
        }

        private async Task PutQueueRabbitMQAsync(string exchange, string queueName, string node, string vhost = "%2f")
        {
            if (vhost == "/")
                vhost = "%2f";

            var createExchangeContent = new StringContent("{\"type\":\"x-consistent-hash\",\"auto_delete\":false,\"durable\":true,\"internal\":false,\"arguments\":{}}", Encoding.UTF8, "application/json");
            var createExchangeResponse = await httpClient.PutAsync($"exchanges/{vhost}/{exchange}", createExchangeContent);
            if (!createExchangeResponse.IsSuccessStatusCode)
                throw new Exception("Failed to create exchange");

            var createQueueContent = new StringContent("{\"durable\":true,\"node\":\""+node+"\"}", Encoding.UTF8, "application/json");
            var createQueueResponse = await httpClient.PutAsync($"queues/{vhost}/{queueName}", createQueueContent);
            if (!createQueueResponse.IsSuccessStatusCode)
                throw new Exception("Failed to create queue");

            var createBindingsContent = new StringContent("{\"routing_key\":\"10\",\"arguments\":{}}", Encoding.UTF8, "application/json");
            var createBindingsResponse = await httpClient.PostAsync($"bindings/{vhost}/e/{exchange}/q/{queueName}", createBindingsContent);
            if (!createBindingsResponse.IsSuccessStatusCode)
                throw new Exception("Failed to create exchange to queue bindings");
        }

        private async Task DeleteQueueRabbitMQAsync(string queueName, string vhost = "%2f")
        {
            if (vhost == "/")
                vhost = "%2f";

            await httpClient.DeleteAsync($"queues/{vhost}/{queueName}");
        }
    }
}