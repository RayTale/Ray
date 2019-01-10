using System;
using System.Collections.Concurrent;

namespace Ray.Core.Storage
{
    public class ConfigureBuilderContainer : IConfigureBuilderContainer
    {
        private readonly ConcurrentDictionary<Type, BaseConfigureBuilderWrapper> configBuilderWrapperDict = new ConcurrentDictionary<Type, BaseConfigureBuilderWrapper>();
        public void Register(Type type, BaseConfigureBuilderWrapper builder)
        {
            configBuilderWrapperDict.TryAdd(type, builder);
        }

        public bool TryGetValue(Type type, out BaseConfigureBuilderWrapper builderWrapper)
        {
            return configBuilderWrapperDict.TryGetValue(type, out builderWrapper);
        }
    }
}
