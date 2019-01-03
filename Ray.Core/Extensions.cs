using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
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
            serviceCollection.AddTransient(typeof(IMpscChannel<>), typeof(MpscChannel<>));
            serviceCollection.AddSingleton<IJsonSerializer, DefaultJsonSerializer>();
        }
    }
}
