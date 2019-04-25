using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using RabbitMQ.Client;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;

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
                var groupsConfig = serviceProvider.GetOptionsByName<GroupsOptions>(type.FullName);
                var eventBus = CreateEventBus(string.IsNullOrEmpty(config.Exchange) ? type.Name : config.Exchange, string.IsNullOrEmpty(config.RoutePrefix) ? type.Name : config.RoutePrefix, config.LBCount, config.MinQos, config.IncQos, config.MaxQos, config.AutoAck, config.Reenqueue).BindProducer(type);
                if (typeof(IGrainWithIntegerKey).IsAssignableFrom(type))
                {
                    var observerUnit = observerUnitContainer.GetUnit(type) as IObserverUnit<long>;
                    var groups = observerUnit.GetGroups();
                    foreach (var group in groups)
                    {
                        var groupConfig = groupsConfig.Configs?.SingleOrDefault(c => c.Group == group);
                        eventBus.CreateConsumer<long>(group, groupConfig?.Config);
                    }
                }
                else if (typeof(IGrainWithStringKey).IsAssignableFrom(type))
                {
                    var observerUnit = observerUnitContainer.GetUnit(type) as IObserverUnit<string>;
                    var groups = observerUnit.GetGroups();
                    foreach (var group in groups)
                    {
                        var groupConfig = groupsConfig.Configs?.SingleOrDefault(c => c.Group == group);
                        eventBus.CreateConsumer<string>(group, groupConfig?.Config);
                    }
                }
                else if (typeof(IGrainWithGuidKey).IsAssignableFrom(type))
                {
                    var observerUnit = observerUnitContainer.GetUnit(type) as IObserverUnit<Guid>;
                    var groups = observerUnit.GetGroups();
                    foreach (var group in groups)
                    {
                        var groupConfig = groupsConfig.Configs?.SingleOrDefault(c => c.Group == group);
                        eventBus.CreateConsumer<Guid>(group, groupConfig?.Config);
                    }
                }
                else
                    throw new PrimaryKeyTypeException(type.FullName);
                await Work(eventBus);
            }
        }
        public RabbitEventBus CreateEventBus(string exchange, string routePrefix, int lBCount = 1, ushort minQos = 100, ushort incQos = 100, ushort maxQos = 300, bool autoAck = false, bool reenqueue = false)
        {
            if (string.IsNullOrEmpty(exchange))
                throw new ArgumentNullException(nameof(exchange));
            if (string.IsNullOrEmpty(routePrefix))
                throw new ArgumentNullException(nameof(routePrefix));
            if (lBCount < 1)
                throw new ArgumentOutOfRangeException($"{nameof(lBCount)} must be greater than 1");
            return new RabbitEventBus(serviceProvider, this, exchange, routePrefix, lBCount, minQos, incQos, maxQos, autoAck, reenqueue);
        }
        public RabbitEventBus CreateEventBus<MainGrain>(string exchange, string routePrefix, int lBCount = 1, ushort minQos = 100, ushort incQos = 100, ushort maxQos = 300, bool autoAck = false, bool reenqueue = false)
        {
            return CreateEventBus(exchange, routePrefix, lBCount, minQos, incQos, maxQos, autoAck, reenqueue).BindProducer<MainGrain>();
        }
        public async Task Work(RabbitEventBus bus)
        {
            if (eventBusDictionary.TryAdd(bus.ProducerType, bus))
            {
                eventBusList.Add(bus);
                using (var channel = await rabbitMQClient.PullModel())
                {
                    channel.Model.ExchangeDeclare(bus.Exchange, "direct", true);
                }
            }
            else
                throw new EventBusRepeatException(bus.ProducerType.FullName);
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
