namespace Ray.Core.Serialization
{
    public interface IJsonSerializer
    {
        T Deserialize<T>(string json);
        string Serialize<T>(T data);
    }
}
