using System;

namespace Rebalanser.ZooKeeper.Tests.RandomisedTests.TestComponents
{
    public class ErrorViolation
    {
        public ErrorViolation(string message)
        {
            Message = message;
        }
        
        public ErrorViolation(string message, Exception ex)
        {
            Message = message;
            Exception = ex;
        }

        public string Message { get; set; }
        public Exception Exception { get; set; }

        public override string ToString()
        {
            if (Exception != null)
                return Exception.ToString();

            return Message;
        }
    }
}