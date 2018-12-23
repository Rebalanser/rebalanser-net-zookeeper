using System;

namespace Rebalanser.ZooKeeper.Tests.RandomisedTests.TestComponents
{
    public class AssignmentEvent
    {
        public DateTime EventTime { get; set; }
        public string ClientId { get; set; }
        public string Action { get; set; }
    }
}