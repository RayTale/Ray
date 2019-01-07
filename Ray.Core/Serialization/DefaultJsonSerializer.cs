using Newtonsoft.Json;

namespace Ray.Core.Serialization
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
