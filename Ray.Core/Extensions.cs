using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Client;
using Ray.Core.Messaging;
using Ray.Core.Messaging.Channels;

namespace Ray.Core
{
    public static class Extensions
    {
        public static void AddRay(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IClientFactory, ClientFactory>();
            serviceCollection.AddSingleton(typeof(IMpscChannelFactory<,,>), typeof(MpscChannelFactory<,,>));
            serviceCollection.AddSingleton<IJsonSerializer, DefaultJsonSerializer>();
        }
    }
}
