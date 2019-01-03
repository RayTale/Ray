namespace Ray.Core.Abstractions
{
    public interface IJsonSerializer
    {
        T Deserialize<T>(string json);
        string Serialize<T>(T data);
    }
}
