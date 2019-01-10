using System;

namespace Ray.Core.Storage
{
    public class StorageFactoryContainer : IStorageFactoryContainer
    {
        readonly IConfigureBuilderContainer configureContainer;
        readonly IServiceProvider serviceProvider;
        public StorageFactoryContainer(
            IServiceProvider serviceProvider,
            IConfigureBuilderContainer configureContainer)
        {
            this.serviceProvider = serviceProvider;
            this.configureContainer = configureContainer;
        }
        public IStorageFactory CreateFactory(Type type)
        {
            if (configureContainer.TryGetValue(type, out var value))
            {
                return serviceProvider.GetService(value.FactoryType) as IStorageFactory;
            }
            else
            {
                throw new NotImplementedException($"{nameof(StorageFactoryContainer)} of {type.FullName}");
            }
        }
    }
}
