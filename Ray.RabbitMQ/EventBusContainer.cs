using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using RabbitMQ.Client;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public class EventBusContainer<W> : IRabbitEventBusContainer<W>, IProducerContainer
        where W : IBytesWrapper
    {
        private readonly ConcurrentDictionary<Type, RabbitEventBus<W>> eventBusDictionary = new ConcurrentDictionary<Type, RabbitEventBus<W>>();
        private readonly List<RabbitEventBus<W>> eventBusList = new List<RabbitEventBus<W>>();
        readonly IRabbitMQClient rabbitMQClient;
        readonly IServiceProvider serviceProvider;
        public EventBusContainer(
            IServiceProvider serviceProvider,
            IRabbitMQClient rabbitMQClient)
        {
            this.serviceProvider = serviceProvider;
            this.rabbitMQClient = rabbitMQClient;
        }
        public RabbitEventBus<W> CreateEventBus<K>(string exchange, string queue, int queueCount = 1)
        {
            if (string.IsNullOrEmpty(exchange))
                throw new ArgumentNullException(nameof(exchange));
            if (string.IsNullOrEmpty(queue))
                throw new ArgumentNullException(nameof(queue));
            if (queueCount < 1)
                throw new ArgumentOutOfRangeException($"{nameof(queueCount)} must be greater than 1");
            return new RabbitEventBus<W>(serviceProvider, this, exchange, queue, queueCount);
        }
        public async Task Work(RabbitEventBus<W> bus)
        {
            foreach (var producer in bus.Producers)
            {
                eventBusDictionary.TryAdd(producer, bus);
            }
            eventBusList.Add(bus);
            using (var channel = await rabbitMQClient.PullModel())
            {
                channel.Model.ExchangeDeclare(bus.Exchange, "direct", true);
            }
        }

        readonly ConcurrentDictionary<Type, IProducer> producerDict = new ConcurrentDictionary<Type, IProducer>();
        public ValueTask<IProducer> GetProducer(Grain grain)
        {
            var type = grain.GetType();
            if (eventBusDictionary.TryGetValue(type, out var eventBus))
            {
                return new ValueTask<IProducer>(producerDict.GetOrAdd(type, key =>
                {
                    return new RabbitMQService<W>(rabbitMQClient, eventBus);
                }));
            }
            else
            {
                throw new NotImplementedException($"{nameof(IProducer)} of {type.FullName}");
            }
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
