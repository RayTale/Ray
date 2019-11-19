using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using RabbitMQ.Client;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public class EventBusContainer : IRabbitEventBusContainer, IProducerContainer
    {
        private readonly ConcurrentDictionary<Type, RabbitEventBus> eventBusDictionary = new ConcurrentDictionary<Type, RabbitEventBus>();
        private readonly List<RabbitEventBus> eventBusList = new List<RabbitEventBus>();
        readonly IRabbitMQClient rabbitMQClient;
        readonly IServiceProvider serviceProvider;
        private readonly IObserverUnitContainer observerUnitContainer;
        public EventBusContainer(
            IServiceProvider serviceProvider,
            IObserverUnitContainer observerUnitContainer,
            IRabbitMQClient rabbitMQClient)
        {
            this.serviceProvider = serviceProvider;
            this.rabbitMQClient = rabbitMQClient;
            this.observerUnitContainer = observerUnitContainer;
        }
        public async Task AutoRegister()
        {
            var observableList = new List<(Type type, ProducerAttribute config)>();
            foreach (var assembly in AssemblyHelper.GetAssemblies(serviceProvider.GetService<ILogger<EventBusContainer>>()))
            {
                foreach (var type in assembly.GetTypes())
                {
                    foreach (var attribute in type.GetCustomAttributes(false))
                    {
                        if (attribute is ProducerAttribute config)
                        {
                            observableList.Add((type, config));
                            break;
                        }
                    }
                }
            }
            foreach (var (type, config) in observableList)
            {
                var eventBus = CreateEventBus(string.IsNullOrEmpty(config.Exchange) ? type.Name : config.Exchange, string.IsNullOrEmpty(config.RoutePrefix) ? type.Name : config.RoutePrefix, config.LBCount, config.AutoAck, config.Reenqueue, config.Persistent).BindProducer(type);
                if (typeof(IGrainWithIntegerKey).IsAssignableFrom(type))
                {
                    await eventBus.AddGrainConsumer<long>();
                }
                else if (typeof(IGrainWithStringKey).IsAssignableFrom(type))
                {
                    await eventBus.AddGrainConsumer<string>();
                }
                else
                    throw new PrimaryKeyTypeException(type.FullName);
            }
        }
        public RabbitEventBus CreateEventBus(string exchange, string routePrefix, int lBCount = 1, bool autoAck = false, bool reenqueue = false, bool persistent = false)
        {
            return new RabbitEventBus(observerUnitContainer, this, exchange, routePrefix, lBCount, autoAck, reenqueue, persistent);
        }
        public RabbitEventBus CreateEventBus<MainGrain>(string exchange, string routePrefix, int lBCount = 1, bool autoAck = false, bool reenqueue = false, bool persistent = false)
        {
            return CreateEventBus(exchange, routePrefix, lBCount, autoAck, reenqueue, persistent).BindProducer<MainGrain>();
        }
        public Task Work(RabbitEventBus bus)
        {
            if (eventBusDictionary.TryAdd(bus.ProducerType, bus))
            {
                eventBusList.Add(bus);
                using var channel = rabbitMQClient.PullModel();
                channel.Model.ExchangeDeclare(bus.Exchange, "direct", true);
                return Task.CompletedTask;
            }
            else
                throw new EventBusRepeatException(bus.ProducerType.FullName);
        }

        readonly ConcurrentDictionary<Type, IProducer> producerDict = new ConcurrentDictionary<Type, IProducer>();
        public ValueTask<IProducer> GetProducer(Type type)
        {
            if (eventBusDictionary.TryGetValue(type, out var eventBus))
            {
                return new ValueTask<IProducer>(producerDict.GetOrAdd(type, key =>
                {
                    return new RabbitProducer(rabbitMQClient, eventBus);
                }));
            }
            else
            {
                throw new NotImplementedException($"{nameof(IProducer)} of {type.FullName}");
            }
        }
        public ValueTask<IProducer> GetProducer<T>()
        {
            return GetProducer(typeof(T));
        }
        public List<IConsumer> GetConsumers()
        {
            var result = new List<IConsumer>();
            foreach (var eventBus in eventBusList)
            {
                result.AddRange(eventBus.Consumers);
            }
            return result;
        }
    }
}
