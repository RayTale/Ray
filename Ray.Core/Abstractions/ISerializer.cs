using System;
using System.IO;

namespace Ray.Core.Abstractions
{
    public interface ISerializer
    {
        object Deserialize(Type type, Stream source);
        T Deserialize<T>(Stream source);
        void Serialize<T>(Stream destination, T instance);
    }
}
