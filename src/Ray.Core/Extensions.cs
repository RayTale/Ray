using Microsoft.Extensions.DependencyInjection;
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
            serviceCollection.AutoAddEventHandler();
            serviceCollection.AddTransient(typeof(IMpscChannel<>), typeof(MpscChannel<>));
            serviceCollection.AddSingleton<ISerializer, DefaultJsonSerializer>();
            serviceCollection.AddSingleton<IObserverUnitContainer, ObserverUnitContainer>();
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
