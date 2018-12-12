using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Client;
using Ray.Core.Messaging;

namespace Ray.Core
{
    public static class Extensions
    {
        public static void AddRay(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IClientFactory, ClientFactory>();
            serviceCollection.AddSingleton<IJsonSerializer, DefaultJsonSerializer>();
        }
    }
}
