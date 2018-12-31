using System;
using Rebalanser.Core.Logging;

namespace Rebalanser.ZooKeeper.Tests.Helpers
{
    public class TestOutputLogger : ILogger
    {
        private static object LockObj = new object();
        
        public void SetMinimumLevel(LogLevel logLevel)
        {
            
        }

        public void Debug(string clientId, string text)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : {clientId} : {text}\r\n");
        }

        public void Info(string clientId, string text)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : {clientId} : {text}\r\n");
        }

        public void Warn(string clientId, string text)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : {clientId} : {text}\r\n");
        }

        public void Error(string clientId, string text)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : {clientId} : {text}\r\n");
        }

        public void Error(string clientId, Exception ex)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : {clientId} : {ex}\r\n");
        }

        public void Error(string clientId, string text, Exception ex)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : {clientId} : {text} : {ex}\r\n");
        }
    }
}