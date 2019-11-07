using System;
using Newtonsoft.Json;

namespace Ray.Core.Serialization
{
    public class DefaultJsonSerializer : ISerializer
    {
        static readonly JsonSerializerSettings options = new JsonSerializerSettings() ;
        public T Deserialize<T>(string json) where T : class, new()
        {
            return JsonConvert.DeserializeObject<T>(json);
        }

        public object Deserialize(byte[] bytes, Type type)
        {
            return JsonConvert.DeserializeObject(System.Text.UTF8Encoding.UTF8.GetString(bytes), type);
        }
        public string Serialize<T>(T data) where T : class, new()
        {
            return JsonConvert.SerializeObject(data, typeof(T), Formatting.None, options);
        }
        public string Serialize(object data, Type type)
        {
            return JsonConvert.SerializeObject(data, type, Formatting.None, options);
        }
        public byte[] SerializeToUtf8Bytes<T>(T data) where T : class, new()
        {
            var s = Serialize<T>(data);
            return System.Text.UTF8Encoding.UTF8.GetBytes(s);
        }

        public T Deserialize<T>(byte[] bytes) where T : class, new()
        {
            return JsonConvert.DeserializeObject<T>(System.Text.UTF8Encoding.UTF8.GetString(bytes));
        }

        public byte[] SerializeToUtf8Bytes(object data, Type type)
        {
            var s = Serialize(data,type);
            return System.Text.UTF8Encoding.UTF8.GetBytes(s);
        }

        public object Deserialize(string json, Type type)
        {
            return JsonConvert.DeserializeObject(json,type);
        }
    }
}
