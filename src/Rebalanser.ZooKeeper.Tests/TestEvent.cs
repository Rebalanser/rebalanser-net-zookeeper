using System.Collections.Generic;

namespace Rebalanser.ZooKeeper.Tests
{
    public enum EventType
    {
        Assignment,
        Unassignment,
        Error
    }
    
    public class TestEvent
    {
        public EventType EventType { get; set; }
        public IList<string> Resources { get; set; }
    }
}