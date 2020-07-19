using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.Utils;

namespace Ray.EventBus.Kafka
{
    public class EventBusContainer : IKafkaEventBusContainer, IProducerContainer
    {
        private readonly ConcurrentDictionary<Type, KafkaEventBus> eventBusDictionary = new ConcurrentDictionary<Type, KafkaEventBus>();
        private readonly List<KafkaEventBus> eventBusList = new List<KafkaEventBus>();
        private readonly IKafkaClient client;
        private readonly IObserverUnitContainer observerUnitContainer;

        public EventBusContainer(
            IObserverUnitContainer observerUnitContainer,
            IKafkaClient client)
        {
            this.client = client;
            this.observerUnitContainer = observerUnitContainer;
        }

        public async Task AutoRegister()
        {
            var observableList = new List<(Type type, ProducerAttribute config)>();
            foreach (var assembly in AssemblyHelper.GetAssemblies())
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
                var eventBus = this.CreateEventBus(string.IsNullOrEmpty(config.Topic) ? type.Name : config.Topic, config.LBCount, config.RetryCount, config.RetryIntervals).BindProducer(type);
                if (typeof(IGrainWithIntegerKey).IsAssignableFrom(type))
                {
                    await eventBus.AddGrainConsumer<long>();
                }
                else if (typeof(IGrainWithStringKey).IsAssignableFrom(type))
                {
                    await eventBus.AddGrainConsumer<string>();
                }
                else
                {
                    throw new PrimaryKeyTypeException(type.FullName);
                }
            }
        }

        public KafkaEventBus CreateEventBus(string topic, int lBCount = 1, int retryCount = 3, int retryIntervals = 500)
        {
            return new KafkaEventBus(this.observerUnitContainer, this, topic, lBCount, retryCount, retryIntervals);
        }

        public KafkaEventBus CreateEventBus<MainGrain>(string topic, int lBCount = 1, int retryCount = 3, int retryIntervals = 500)
        {
            return this.CreateEventBus(topic, lBCount, retryCount, retryIntervals).BindProducer<MainGrain>();
        }

        public Task Work(KafkaEventBus bus)
        {
            if (this.eventBusDictionary.TryAdd(bus.ProducerType, bus))
            {
                this.eventBusList.Add(bus);
            }
            else
            {
                throw new EventBusRepeatException(bus.ProducerType.FullName);
            }

            return Task.CompletedTask;
        }

        private readonly ConcurrentDictionary<Type, IProducer> producerDict = new ConcurrentDictionary<Type, IProducer>();

        public ValueTask<IProducer> GetProducer(Type type)
        {
            if (this.eventBusDictionary.TryGetValue(type, out var eventBus))
            {
                return new ValueTask<IProducer>(this.producerDict.GetOrAdd(type, key =>
                {
                    return new KafkaProducer(this.client, eventBus);
                }));
            }
            else
            {
                throw new NotImplementedException($"{nameof(IProducer)} of {type.FullName}");
            }
        }

        public ValueTask<IProducer> GetProducer<T>()
        {
            return this.GetProducer(typeof(T));
        }

        public List<IConsumer> GetConsumers()
        {
            var result = new List<IConsumer>();
            foreach (var eventBus in this.eventBusList)
            {
                result.AddRange(eventBus.Consumers);
            }

            return result;
        }
    }
}
