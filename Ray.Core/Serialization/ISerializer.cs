using System;

namespace Ray.Core.Serialization
{
    public interface ISerializer
    {
        object Deserialize(Type type, byte[] bytes);
        T Deserialize<T>(byte[] bytes);
        T Deserialize<T>(string json);
        string SerializeToString<T>(T data);
        byte[] SerializeToBytes<T>(T data);
    }
}
