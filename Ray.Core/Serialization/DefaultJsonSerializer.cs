using System;
using System.Text;
using SpanJson;

namespace Ray.Core.Serialization
{
    public class DefaultJsonSerializer : ISerializer
    {
        public T Deserialize<T>(string json)
        {
            return JsonSerializer.Generic.Utf8.Deserialize<T>(Encoding.Default.GetBytes(json));
        }

        public object Deserialize(Type type, byte[] bytes)
        {
            return JsonSerializer.NonGeneric.Utf8.Deserialize(bytes, type);
        }

        public string SerializeToString<T>(T data)
        {
            return Encoding.Default.GetString(JsonSerializer.NonGeneric.Utf8.SerializeToArrayPool(data));
        }

        public byte[] SerializeToBytes<T>(T data)
        {
            return JsonSerializer.NonGeneric.Utf8.Serialize(data);
        }

        public T Deserialize<T>(byte[] bytes)
        {
            return JsonSerializer.Generic.Utf8.Deserialize<T>(bytes);
        }
    }
}
