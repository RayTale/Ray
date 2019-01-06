using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.Client;
using Ray.Core.Internal.Serializer;
using Ray.Core.Internal.Channels;

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
    }
}
