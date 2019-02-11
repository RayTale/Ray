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
        private static void AddRay(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddTransient(typeof(IMpscChannel<>), typeof(MpscChannel<>));
            serviceCollection.AddSingleton<ISerializer, DefaultJsonSerializer>();
            serviceCollection.AddSingleton<IConfigureBuilderContainer, ConfigureBuilderContainer>();
            serviceCollection.AddSingleton<IStorageFactoryContainer, StorageFactoryContainer>();
            serviceCollection.AddSingleton<IFollowUnitContainer, FollowUnitContainer>();
        }
        public static IClientBuilder AddRay(this IClientBuilder clientBuilder)
        {

            clientBuilder.ConfigureServices((context, servicecollection) => servicecollection.AddRay());
            return clientBuilder;
        }
        public static ISiloHostBuilder AddRay(this ISiloHostBuilder siloHostBuilder)
        {
            siloHostBuilder.ConfigureServices((context, servicecollection) => servicecollection.AddRay());
            siloHostBuilder.AddStartupTask<SiloStartupTask>();
            return siloHostBuilder;
        }
    }
}
