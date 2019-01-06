using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public static class Extensions
    {
        public static void AddRabbitMQ<W>(this IServiceCollection serviceCollection)
            where W : IBytesWrapper
        {
            serviceCollection.AddSingleton<IRabbitMQClient, RabbitMQClient>();
            serviceCollection.AddSingleton<IConsumerManager, ConsumerManager<W>>();
            serviceCollection.AddSingleton<IRabbitEventBusContainer<W>, EventBusContainer<W>>();
            serviceCollection.AddSingleton(serviceProvider => serviceProvider.GetService<IRabbitEventBusContainer<W>>() as IProducerContainer);
        }
    }
}
