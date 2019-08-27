using System;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;

namespace Ray.Core.Serialization
{
    public class DefaultJsonSerializer : ISerializer
    {
        static readonly JsonSerializerOptions options = new JsonSerializerOptions() { Encoder = JavaScriptEncoder.Create(UnicodeRanges.All) };
        public T Deserialize<T>(string json)
        {
            return JsonSerializer.Deserialize<T>(json);
        }

        public object Deserialize(Type type, byte[] bytes)
        {
            return JsonSerializer.Deserialize(bytes, type);
        }

        public string Serialize<T>(T data)
        {
            return JsonSerializer.Serialize(data, options);
        }

        public byte[] SerializeToUtf8Bytes<T>(T data)
        {
            return JsonSerializer.SerializeToUtf8Bytes(data, options);
        }

        public T Deserialize<T>(byte[] bytes)
        {
            return JsonSerializer.Deserialize<T>(bytes);
        }
    }
}
