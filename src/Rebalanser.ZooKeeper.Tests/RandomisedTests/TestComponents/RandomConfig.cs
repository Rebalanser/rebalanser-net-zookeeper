using System;

namespace Rebalanser.ZooKeeper.Tests.RandomisedTests.TestComponents
{
    public class RandomConfig
    {
        public RandomConfig()
        {
            SessionTimeout = TimeSpan.FromSeconds(20);
            ConnectTimeout = TimeSpan.FromSeconds(20);
            MinimumRebalancingInterval = TimeSpan.FromSeconds(20);
            StartUpClientInterval = TimeSpan.Zero;
            OnStartEventHandlerTime = TimeSpan.Zero;
            OnStopEventHandlerTime = TimeSpan.Zero;
            RandomiseEventHandlerTimes = false;
            OnAssignmentDelay = TimeSpan.Zero;
        }
        
        public TimeSpan SessionTimeout { get; set; }
        public TimeSpan ConnectTimeout { get; set; }
        public TimeSpan MinimumRebalancingInterval { get; set; }
        public TimeSpan StartUpClientInterval { get; set; }
        public int ClientCount { get; set; }
        public int ResourceCount { get; set; }
        public TimeSpan TestDuration { get; set; }
        public CheckType CheckType { get; set; }
        public bool RandomiseInterval { get; set; }
        public TimeSpan MaxInterval { get; set; }
        public int ConditionalCheckInterval { get; set; }
        public TimeSpan ConditionalCheckWaitPeriod { get; set; }
        public TimeSpan OnStartEventHandlerTime { get; set; }
        public TimeSpan OnStopEventHandlerTime { get; set; }
        public TimeSpan OnAssignmentDelay { get; set; }
        public bool RandomiseEventHandlerTimes { get; set; }
        
    }
}