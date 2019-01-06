using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.Client;
using Ray.Core.Internal.Channels;
using Ray.Core.Internal.Serializer;

namespace Ray.Core
{
    public static class Extensions
    {
        public static void AddRay(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IClusterClientFactory, ClusterClientFactory>();
            serviceCollection.AddTransient(typeof(IMpscChannel<>), typeof(MpscChannel<>));
            serviceCollection.AddSingleton<IJsonSerializer, DefaultJsonSerializer>();
        }
        public static Task StartRay(this IServiceProvider serviceProvider)
        {
            return Startup.Start(serviceProvider);
        }
    }
}
