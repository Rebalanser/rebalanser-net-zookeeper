using System;
using System.Collections.Generic;
using System.Linq;

namespace Rebalanser.ZooKeeper.Tests.Helpers
{
//    public class ResourceMonitor
//    {
//        private static Dictionary<string, string> Resources = new Dictionary<string, string>();
//        private static object LockObj = new object();
//
//        public static bool Consume(string clientId, string resource)
//        {
//            lock (LockObj)
//            {
//                if (Resources.ContainsKey(resource))
//                {
//                    return false;
//                }
//                else
//                {
//                    Resources.Add(resource, clientId);
//                    return true;
//                }
//            }
//        }
//        
//        public static void Release(string clientId)
//        {
//            lock (LockObj)
//            {
//                var keyValues = Resources.ToList();
//                foreach(var kv in keyValues)
//                {
//                    if(kv.Value.Equals(clientId, StringComparison.Ordinal))
//                        Resources.Remove(kv.Key);
//                }
//            }
//        }
//    }
}