using System;

namespace Rebalanser.ZooKeeper.Tests.RandomisedTests.TestComponents
{
    public class ReleaseViolation
    {
        public ReleaseViolation(string resourceName,
            string currentlyAssignedClientId,
            string releaserClientId)
        {
            ResourceName = resourceName;
            CurrentlyAssignedClientId = currentlyAssignedClientId;
            ReleaserClientId = releaserClientId;
        }

        public string ResourceName { get; set; }
        public string CurrentlyAssignedClientId { get; set; }
        public string ReleaserClientId { get; set; }

        public override string ToString()
        {
            return
                $"Client {ReleaserClientId} tried to release the resource {ResourceName} assigned to {CurrentlyAssignedClientId}";
        }
    }
}