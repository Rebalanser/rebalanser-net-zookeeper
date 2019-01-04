using System;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;

namespace Rebalanser.ZooKeeper.Zk
{
    public static class JSONSerializer<TType> where TType : class
    {
        /// <summary>
        /// Serializes an object to JSON
        /// </summary>
        public static string Serialize(TType instance)
        {
            var serializer = new DataContractJsonSerializer(typeof(TType));
            using (var stream = new MemoryStream())
            {
                serializer.WriteObject(stream, instance);
                var ser = Encoding.UTF8.GetString(stream.ToArray());
                return ser;
            }
        }

        /// <summary>
        /// DeSerializes an object from JSON
        /// </summary>
        public static TType DeSerialize(string json)
        {
            if(string.IsNullOrWhiteSpace(json))
                return default(TType);
               
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                stream.Position = 0;
                var serializer = new DataContractJsonSerializer(typeof(TType));
                return serializer.ReadObject(stream) as TType;
            }
        }
    }
}