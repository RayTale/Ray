using Microsoft.Extensions.DependencyInjection;
using Ray.Core.MQ;

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
