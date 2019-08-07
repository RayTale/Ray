﻿using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.EventBus;
using Ray.EventBus.Kafka;

namespace Ray.EventBus.RabbitMQ
{
    public static class Extensions
    {
        public static void AddKafkaMQ(
            this IServiceCollection serviceCollection,
            Action<RayKafkaOptions> ConfigAction,
            Action<ProducerConfig> producerConfigAction,
            Action<ConsumerConfig> consumerConfigAction,
            Func<IKafkaEventBusContainer, Task> eventBusConfigFunc = default)
        {
            serviceCollection.Configure<ProducerConfig>(config => producerConfigAction(config));
            serviceCollection.Configure<ConsumerConfig>(config => consumerConfigAction(config));
            serviceCollection.Configure<RayKafkaOptions>(config => ConfigAction(config));
            serviceCollection.AddSingleton<IKafkaClient, KafkaClient>();
            serviceCollection.AddSingleton<IConsumerManager, ConsumerManager>();
            serviceCollection.AddSingleton<IKafkaEventBusContainer, EventBusContainer>();
            serviceCollection.AddSingleton(serviceProvider => serviceProvider.GetService<IKafkaEventBusContainer>() as IProducerContainer);
            Startup.Register(async serviceProvider =>
            {
                var container = serviceProvider.GetService<IKafkaEventBusContainer>();
                if (eventBusConfigFunc != default)
                    await eventBusConfigFunc(container);
                await container.AutoRegister();
            });
        }
    }
}
