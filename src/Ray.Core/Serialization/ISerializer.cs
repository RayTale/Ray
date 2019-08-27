using System;

namespace Ray.Core.Serialization
{
    public interface ISerializer
    {
        object Deserialize(Type type, byte[] bytes);
        T Deserialize<T>(byte[] bytes);
        T Deserialize<T>(string json);
        string Serialize<T>(T data);
        byte[] SerializeToUtf8Bytes<T>(T data);
    }
}
