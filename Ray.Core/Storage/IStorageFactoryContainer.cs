using System;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IStorageFactoryContainer
    {
        IStorageFactory CreateFactory(Type type);
    }
}
