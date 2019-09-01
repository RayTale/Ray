using System;

namespace Ray.Core.Serialization
{
    public interface ISerializer
    {
        object Deserialize(byte[] bytes, Type type);
        T Deserialize<T>(byte[] bytes) where T : class, new();
        T Deserialize<T>(string json) where T : class, new();
        object Deserialize(string json, Type type);
        string Serialize<T>(T data) where T : class, new();
        string Serialize(object data, Type type);
        byte[] SerializeToUtf8Bytes<T>(T data) where T : class, new();
        byte[] SerializeToUtf8Bytes(object data, Type type);
    }
}
