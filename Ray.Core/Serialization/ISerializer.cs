using System;
using System.IO;
using System.Threading.Tasks;

namespace Ray.Core.Serialization
{
    public interface ISerializer
    {
        object Deserialize(Type type, Stream source);
        T Deserialize<T>(Stream source);
        void Serialize<T>(Stream destination, T instance);
    }
}
