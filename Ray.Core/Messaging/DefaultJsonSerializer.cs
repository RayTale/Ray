using Newtonsoft.Json;
using Ray.Core.Abstractions;

namespace Ray.Core.Messaging
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
