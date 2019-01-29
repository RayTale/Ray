using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public class EventBusContainer : IRabbitEventBusContainer, IProducerContainer
    {
        private readonly ConcurrentDictionary<Type, RabbitEventBus> eventBusDictionary = new ConcurrentDictionary<Type, RabbitEventBus>();
        private readonly List<RabbitEventBus> eventBusList = new List<RabbitEventBus>();
        readonly IRabbitMQClient rabbitMQClient;
        readonly IServiceProvider serviceProvider;
        public EventBusContainer(
            IServiceProvider serviceProvider,
            IRabbitMQClient rabbitMQClient)
        {
            this.serviceProvider = serviceProvider;
            this.rabbitMQClient = rabbitMQClient;
        }
        public RabbitEventBus CreateEventBus(string exchange, string queue, int queueCount = 1)
        {
            if (string.IsNullOrEmpty(exchange))
                throw new ArgumentNullException(nameof(exchange));
            if (string.IsNullOrEmpty(queue))
                throw new ArgumentNullException(nameof(queue));
            if (queueCount < 1)
                throw new ArgumentOutOfRangeException($"{nameof(queueCount)} must be greater than 1");
            return new RabbitEventBus(serviceProvider, this, exchange, queue, queueCount);
        }
        public RabbitEventBus CreateEventBus<MainGrain>(string exchange, string queue, int queueCount = 1)
        {
            return CreateEventBus(exchange, queue, queueCount).BindProducer<MainGrain>();
        }
        public async Task Work(RabbitEventBus bus)
        {
            eventBusDictionary.TryAdd(bus.ProducerType, bus);
            eventBusList.Add(bus);
            using (var channel = await rabbitMQClient.PullModel())
            {
                channel.Model.ExchangeDeclare(bus.Exchange, "direct", true);
            }
        }

        readonly ConcurrentDictionary<Type, IProducer> producerDict = new ConcurrentDictionary<Type, IProducer>();
        public ValueTask<IProducer> GetProducer<T>(T data)
        {
            var type = data.GetType();
            if (eventBusDictionary.TryGetValue(type, out var eventBus))
            {
                return new ValueTask<IProducer>(producerDict.GetOrAdd(type, key =>
                {
                    return new RabbitProducer(rabbitMQClient, eventBus, type);
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
