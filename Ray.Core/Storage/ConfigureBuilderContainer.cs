using System;
using System.Collections.Concurrent;
using Ray.Core.Exceptions;

namespace Ray.Core.Storage
{
    public class ConfigureBuilderContainer : IConfigureBuilderContainer
    {
        private readonly ConcurrentDictionary<Type, BaseConfigureBuilderWrapper> configBuilderWrapperDict = new ConcurrentDictionary<Type, BaseConfigureBuilderWrapper>();
        public void Register(Type type, BaseConfigureBuilderWrapper builder)
        {
            if (!configBuilderWrapperDict.TryAdd(type, builder))
                throw new StorageConfigureBuilderReRegisterException(type.FullName);
        }

        public bool TryGetValue(Type type, out BaseConfigureBuilderWrapper builderWrapper)
        {
            return configBuilderWrapperDict.TryGetValue(type, out builderWrapper);
        }
    }
}
