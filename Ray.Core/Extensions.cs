using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
using Ray.Core.Abstractions;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Serialization;
using Ray.Core.Storage;

namespace Ray.Core
{
    public static class Extensions
    {
        private static void AddRay<StartupConfig>(this IServiceCollection serviceCollection)
            where StartupConfig : IStartupConfig, new()
        {
            var startupConfig = new StartupConfig();
            startupConfig.Configure(serviceCollection);
            serviceCollection.AddSingleton<IStartupConfig>(startupConfig);
            serviceCollection.AddEventHandler();
            serviceCollection.AddTransient(typeof(IMpscChannel<>), typeof(MpscChannel<>));
            serviceCollection.AddSingleton<ISerializer, DefaultJsonSerializer>();
            serviceCollection.AddSingleton<IConfigureBuilderContainer, ConfigureBuilderContainer>();
            serviceCollection.AddSingleton<IStorageFactoryContainer, StorageFactoryContainer>();
            serviceCollection.AddSingleton<IFollowUnitContainer, FollowUnitContainer>();
        }
        public static ISiloHostBuilder AddRay<StartupConfig>(this ISiloHostBuilder siloHostBuilder)
            where StartupConfig : IStartupConfig, new()
        {
            siloHostBuilder.ConfigureServices((context, servicecollection) => servicecollection.AddRay<StartupConfig>());
            siloHostBuilder.AddStartupTask<SiloStartupTask>();
            return siloHostBuilder;
        }
    }
}
