using System.Collections.Generic;

namespace Rebalanser.ZooKeeper.Store
{
    public class GetResourcesResponse
    {
        public AssignmentStatus AssignmentStatus { get; set; }
        public List<string> Resources { get; set; }
    }
}