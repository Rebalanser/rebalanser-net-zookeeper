using System.Collections.Generic;

namespace Rebalanser.ZooKeeper.Store
{
    class GetResourcesRequest
    {
        public AssignmentStatus ResourceAssignmentStatus { get; set; }
        public List<string> Resources { get; set; }
    }
}