using System.Collections.Generic;
using System.Linq;

namespace Rebalanser.ZooKeeper.Zk
{
    public class ResourcesZnode
    {
        public ResourcesZnode()
        {
            ResourceAssignments = new ResourcesZnodeData();
        }
        
        public ResourcesZnodeData ResourceAssignments { get; set; }
        public List<string> Resources { get; set; } 
        public int Version { get; set; }

        public bool HasResourceChange()
        {
            if (ResourceAssignments.Assignments.Count != Resources.Count)
                return true;

            return !ResourceAssignments.Assignments
                .Select(x => x.Resource)
                .OrderBy(x => x)
                .SequenceEqual(Resources.OrderBy(x => x));
        }
    }
}