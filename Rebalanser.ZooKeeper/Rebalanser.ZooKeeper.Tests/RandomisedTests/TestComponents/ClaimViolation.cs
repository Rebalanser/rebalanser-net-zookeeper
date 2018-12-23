using System;

namespace Rebalanser.ZooKeeper.Tests.RandomisedTests.TestComponents
{
    public class ClaimViolation
    {
        public ClaimViolation(string resourceName,
            string currentlyAssignedClientId,
            string claimerClientId)
        {
            ResourceName = resourceName;
            CurrentlyAssignedClientId = currentlyAssignedClientId;
            ClaimerClientId = claimerClientId;
        }

        public string ResourceName { get; set; }
        public string CurrentlyAssignedClientId { get; set; }
        public string ClaimerClientId { get; set; }

        public override string ToString()
        {
            return
                $"Client {ClaimerClientId} tried to claim the resource {ResourceName} still assigned to {CurrentlyAssignedClientId}";
        }
    }
}