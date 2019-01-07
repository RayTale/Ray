using System;
using System.IO;
using ProtoBuf;
using Ray.Core.Serialization;

namespace RayTest.IGrains
{
    public class ProtobufSerializer : ISerializer
    {
        public object Deserialize(Type type, Stream source)
        {
            return Serializer.Deserialize(type, source);
        }

        public T Deserialize<T>(Stream source)
        {
            return Serializer.Deserialize<T>(source);
        }

        public void Serialize<T>(Stream destination, T instance)
        {
            Serializer.Serialize(destination, instance);
        }
    }
}
