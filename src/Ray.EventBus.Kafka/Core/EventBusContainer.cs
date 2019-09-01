using Orleans;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public class EventBusContainer : IKafkaEventBusContainer, IProducerContainer
    {
        private readonly ConcurrentDictionary<Type, KafkaEventBus> eventBusDictionary = new ConcurrentDictionary<Type, KafkaEventBus>();
        private readonly List<KafkaEventBus> eventBusList = new List<KafkaEventBus>();
        readonly IKafkaClient Client;
        readonly IServiceProvider serviceProvider;
        private readonly IObserverUnitContainer observerUnitContainer;
        public EventBusContainer(
            IServiceProvider serviceProvider,
            IObserverUnitContainer observerUnitContainer,
            IKafkaClient client)
        {
            this.serviceProvider = serviceProvider;
            Client = client;
            this.observerUnitContainer = observerUnitContainer;
        }
        public async Task AutoRegister()
        {
            var observableList = new List<(Type type, ProducerAttribute config)>();
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
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
                var eventBus = CreateEventBus(string.IsNullOrEmpty(config.Topic) ? type.Name : config.Topic, config.LBCount, config.Reenqueue).BindProducer(type);
                if (typeof(IGrainWithIntegerKey).IsAssignableFrom(type))
                {
                    var observerUnit = observerUnitContainer.GetUnit(type) as IObserverUnit<long>;
                    var groups = observerUnit.GetGroups();
                    foreach (var group in groups)
                    {
                        eventBus.CreateConsumer<long>(group);
                    }
                }
                else if (typeof(IGrainWithStringKey).IsAssignableFrom(type))
                {
                    var observerUnit = observerUnitContainer.GetUnit(type) as IObserverUnit<string>;
                    var groups = observerUnit.GetGroups();
                    foreach (var group in groups)
                    {
                        eventBus.CreateConsumer<string>(group);
                    }
                }
                else
                    throw new PrimaryKeyTypeException(type.FullName);
                await Work(eventBus);
            }
        }
        public KafkaEventBus CreateEventBus(string topic, int lBCount = 1, bool reenqueue = true)
        {
            return new KafkaEventBus(serviceProvider, this, topic, lBCount, reenqueue);
        }
        public KafkaEventBus CreateEventBus<MainGrain>(string topic, int lBCount = 1, bool reenqueue = true)
        {
            return CreateEventBus(topic, lBCount, reenqueue).BindProducer<MainGrain>();
        }
        public Task Work(KafkaEventBus bus)
        {
            if (eventBusDictionary.TryAdd(bus.ProducerType, bus))
            {
                eventBusList.Add(bus);
            }
            else
                throw new EventBusRepeatException(bus.ProducerType.FullName);
            return Task.CompletedTask;
        }

        readonly ConcurrentDictionary<Type, IProducer> producerDict = new ConcurrentDictionary<Type, IProducer>();
        public ValueTask<IProducer> GetProducer(Type type)
        {
            if (eventBusDictionary.TryGetValue(type, out var eventBus))
            {
                return new ValueTask<IProducer>(producerDict.GetOrAdd(type, key =>
                {
                    return new KafkaProducer(Client, eventBus);
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
