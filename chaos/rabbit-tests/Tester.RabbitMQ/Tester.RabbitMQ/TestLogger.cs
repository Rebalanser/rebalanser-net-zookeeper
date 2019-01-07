using System;
using Rebalanser.Core.Logging;

namespace Tester.RabbitMQ
{
    public class TestLogger : ILogger
    {
        private static object LockObj = new object();
        
        public void SetMinimumLevel(LogLevel logLevel)
        {
            
        }

        public void Debug(string clientId, string text)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : DEBUG : {clientId} : {text}\r\n");
        }

        public void Info(string clientId, string text)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : INFO : {clientId} : {text}\r\n");
        }

        public void Warn(string clientId, string text)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : WARN : {clientId} : {text}\r\n");
        }

        public void Error(string clientId, string text)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : ERROR : {clientId} : {text}\r\n");
        }

        public void Error(string clientId, Exception ex)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : ERROR : {clientId} : {ex}\r\n");
        }

        public void Error(string clientId, string text, Exception ex)
        {
            lock(LockObj)
                Console.Write($"{DateTime.Now.ToString("hh:mm:ss,fff")} : ERROR : {clientId} : {text} : {ex}\r\n");
        }
    }
}