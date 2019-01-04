using System;
using System.Runtime.Serialization;

namespace Rebalanser.ZooKeeper.Zk
{
    [Serializable, DataContract(Namespace = "Rebalanser", Name = "ResourceAssignment")]
    public class ResourceAssignment
    {
        [DataMember(Name = "r")]
        public string Resource { get; set; }
        [DataMember(Name = "c")]
        public string ClientId { get; set; }
    }
}