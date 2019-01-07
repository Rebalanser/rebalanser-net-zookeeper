using System.Threading;
using System.Threading.Tasks;

namespace Tester.RabbitMQ
{
    public class ConsumerTask
    {
        public CancellationTokenSource Cts { get; set; }
        public Task Consumer { get; set; }
    }
}