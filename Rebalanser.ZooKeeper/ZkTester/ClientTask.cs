using System.Threading;
using System.Threading.Tasks;

namespace ZkTester
{
    public class ClientTask
    {
        public string Id { get; set; }
        public CancellationTokenSource Cts { get; set; }
        public Task Client { get; set; }
    }
}