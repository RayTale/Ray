using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
using Ray.Core.Abstractions;
using Ray.Core.Channels;
using Ray.Core.Serialization;
using Ray.Core.Storage;

namespace Ray.Core
{
    public static class Extensions
    {
        private static void AddRay<W>(this IServiceCollection serviceCollection)
            where W : class, IBytesWrapper
        {
            serviceCollection.AddTransient<IBytesWrapper, W>();
            serviceCollection.AddTransient(typeof(IMpscChannel<>), typeof(MpscChannel<>));
            serviceCollection.AddSingleton<IJsonSerializer, DefaultJsonSerializer>();
            serviceCollection.AddSingleton<IConfigureBuilderContainer, ConfigureBuilderContainer>();
            serviceCollection.AddSingleton<IStorageFactoryContainer, StorageFactoryContainer>();
            serviceCollection.AddSingleton<IFollowUnitContainer, FollowUnitContainer>();
        }
        public static IClientBuilder AddRay<W>(this IClientBuilder clientBuilder)
            where W : class, IBytesWrapper
        {

            clientBuilder.ConfigureServices((context, servicecollection) => servicecollection.AddRay<W>());
            return clientBuilder;
        }
        public static ISiloHostBuilder AddRay<W>(this ISiloHostBuilder siloHostBuilder)
            where W : class, IBytesWrapper
        {
            siloHostBuilder.ConfigureServices((context, servicecollection) => servicecollection.AddRay<W>());
            siloHostBuilder.AddStartupTask<SiloStartupTask>();
            return siloHostBuilder;
        }
    }
}
