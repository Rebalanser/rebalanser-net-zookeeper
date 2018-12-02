namespace Rebalanser.ZooKeeper.Zk
{
    public class ZkResponse<T>
    {
        public ZkResponse(ZkResult result)
        {
            this.Result = result;
        }
        
        public ZkResponse(ZkResult result, T data)
        {
            this.Result = result;
            this.Data = data;
        }
        
        public ZkResult Result { get; set; }
        public T Data { get; set; }
    }
    
    public class ZkResponse
    {
        public ZkResponse(ZkResult result)
        {
            this.Result = result;
        }
        
        public ZkResult Result { get; set; }
    }
}