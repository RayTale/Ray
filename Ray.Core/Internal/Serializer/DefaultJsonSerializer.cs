using Newtonsoft.Json;
using Ray.Core.Abstractions;

namespace Ray.Core.Internal.Serializer
{
    public class DefaultJsonSerializer : IJsonSerializer
    {
        public T Deserialize<T>(string json)
        {
            return JsonConvert.DeserializeObject<T>(json);
        }

        public string Serialize<T>(T data)
        {
            return JsonConvert.SerializeObject(data);
        }
    }
}
