using System;
using System.Collections.Concurrent;

namespace Ray.Core.Storage
{
    public interface IConfigureContainer<C, P>
    {
        ConcurrentDictionary<Type, object> ConfigBuilderDict { get; }
        void RegisterBuilder<K>(Type type, ConfigureBuilderWrapper<K, C, P> builder);
    }
}
