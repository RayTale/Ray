using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public static class Extensions
    {
        public static void AddRabbitMQ(
            this IServiceCollection serviceCollection,
            Action<RabbitOptions> rabbitConfigAction,
            Func<IRabbitEventBusContainer, Task> eventBusConfig = default)
        {
            serviceCollection.Configure<RabbitOptions>(config => rabbitConfigAction(config));
            serviceCollection.AddSingleton<IRabbitMQClient, RabbitMQClient>();
            serviceCollection.AddHostedService<ConsumerManager>();
            serviceCollection.AddSingleton<IRabbitEventBusContainer, EventBusContainer>();
            serviceCollection.AddSingleton(serviceProvider => serviceProvider.GetService<IRabbitEventBusContainer>() as IProducerContainer);
            Startup.Register(async serviceProvider =>
            {
                var container = serviceProvider.GetService<IRabbitEventBusContainer>();
                if (eventBusConfig != default)
                    await eventBusConfig(container);
                else
                    await container.AutoRegister();
            });
        }
    }
}
