using System;
using System.IO;
using Ray.Core.Serialization;
using SpanJson;

namespace Ray.IGrains
{
    public class SpanJsonSerializer : ISerializer
    {
        public object Deserialize(Type type, Stream source)
        {
            return JsonSerializer.NonGeneric.Utf8.DeserializeAsync(source, type).Result;
        }

        public T Deserialize<T>(Stream source)
        {
            return JsonSerializer.Generic.Utf8.DeserializeAsync<T>(source).Result;
        }

        public void Serialize<T>(Stream destination, T instance)
        {
            JsonSerializer.NonGeneric.Utf8.SerializeAsync(instance, destination).GetAwaiter().GetResult();
        }
    }
}
