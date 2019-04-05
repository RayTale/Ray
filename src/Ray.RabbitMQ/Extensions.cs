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
            Func<IRabbitEventBusContainer, Task> eventBusConfigFunc = default)
        {
            serviceCollection.Configure<RabbitOptions>(config => rabbitConfigAction(config));
            serviceCollection.AddSingleton<IRabbitMQClient, RabbitMQClient>();
            serviceCollection.AddSingleton<IConsumerManager, ConsumerManager>();
            serviceCollection.AddSingleton<IRabbitEventBusContainer, EventBusContainer>();
            serviceCollection.AddSingleton(serviceProvider => serviceProvider.GetService<IRabbitEventBusContainer>() as IProducerContainer);
            Startup.Register(async serviceProvider =>
            {
                var container = serviceProvider.GetService<IRabbitEventBusContainer>();
                if (eventBusConfigFunc != default)
                    await eventBusConfigFunc(container);
                await container.AutoRegister();
            });
        }
    }
}
