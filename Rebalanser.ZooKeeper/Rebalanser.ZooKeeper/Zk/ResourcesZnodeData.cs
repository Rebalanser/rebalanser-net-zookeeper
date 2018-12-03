using System.Collections.Generic;

namespace Rebalanser.ZooKeeper.Zk
{
    public class ResourcesZnodeData
    {
        public ResourcesZnodeData()
        {
            Assignments = new List<ResourceAssignment>();
        }
        
        public List<ResourceAssignment> Assignments { get; set; }
    }
}