using System;
using System.Collections.Concurrent;

namespace Ray.Core.Storage
{
    public class ConfigureContainer<C, P> : IConfigureContainer<C, P>
    {
        public ConcurrentDictionary<Type, object> ConfigBuilderDict { get; } = new ConcurrentDictionary<Type, object>();
        public void RegisterBuilder<K>(Type type, ConfigureBuilderWrapper<K, C, P> builder)
        {
            ConfigBuilderDict.TryAdd(type, builder);
        }
    }
}
