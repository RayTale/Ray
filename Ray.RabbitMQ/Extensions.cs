using Microsoft.Extensions.DependencyInjection;
using Ray.Core.EventBus;

namespace Ray.RabbitMQ
{
    public static class Extensions
    {
        public static void AddRabbitMQ(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IRabbitMQClient, RabbitMQClient>();
            serviceCollection.AddSingleton<ISubManager, RabbitSubManager>();
        }
    }
}
