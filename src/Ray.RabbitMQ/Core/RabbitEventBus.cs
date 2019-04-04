using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.Exceptions;
using Ray.Core.Utils;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitEventBus
    {
        private readonly ConsistentHash _CHash;
        readonly IObserverUnitContainer observerUnitContainer;
        public RabbitEventBus(
            IServiceProvider serviceProvider,
            IRabbitEventBusContainer eventBusContainer,
            string exchange, string routePrefix, int lBCount = 1, ushort minQos = 100, ushort incQos = 100, ushort maxQos = 300, bool autoAck = false, bool reenqueue = false)
        {
            if (string.IsNullOrEmpty(exchange))
                throw new ArgumentNullException(nameof(exchange));
            if (string.IsNullOrEmpty(routePrefix))
                throw new ArgumentNullException(nameof(routePrefix));
            if (lBCount < 1)
                throw new ArgumentOutOfRangeException($"{nameof(lBCount)} must be greater than 1");
            ServiceProvider = serviceProvider;
            observerUnitContainer = serviceProvider.GetService<IObserverUnitContainer>();
            Container = eventBusContainer;
            Exchange = exchange;
            RoutePrefix = routePrefix;
            LBCount = lBCount;
            DefaultConsumerConfig = new ConsumerConfig
            {
                AutoAck = autoAck,
                MaxQos = maxQos,
                MinQos = minQos,
                IncQos = incQos,
                Reenqueue = reenqueue
            };
            RouteList = new List<string>();
            if (LBCount == 1)
            {
                RouteList.Add(routePrefix);
            }
            else
            {
                for (int i = 0; i < LBCount; i++)
                {
                    RouteList.Add($"{routePrefix }_{ i.ToString()}");
                }
            }
            _CHash = new ConsistentHash(RouteList, lBCount * 10);
        }
        public IServiceProvider ServiceProvider { get; }
        public IRabbitEventBusContainer Container { get; }
        public string Exchange { get; }
        public string RoutePrefix { get; }
        public int LBCount { get; }
        public ConsumerConfig DefaultConsumerConfig { get; set; }
        public List<string> RouteList { get; }
        public Type ProducerType { get; set; }
        public List<RabbitConsumer> Consumers { get; set; } = new List<RabbitConsumer>();
        public string GetRoute(string key)
        {
            return LBCount == 1 ? RoutePrefix : _CHash.GetNode(key); ;
        }
        public RabbitEventBus BindProducer<TGrain>()
        {
            return BindProducer(typeof(TGrain));
        }
        public RabbitEventBus BindProducer(Type grainType)
        {
            if (ProducerType == null)
                ProducerType = grainType;
            else
                throw new EventBusRepeatBindingProducerException(grainType.FullName);
            return this;
        }
        public RabbitEventBus CreateConsumer<PrimaryKey>(string observerGroup, ConsumerConfig consumerConfig = default)
        {
            var consumer = new RabbitConsumer(observerUnitContainer.GetUnit<PrimaryKey>(ProducerType).GetEventHandlers(observerGroup))
            {
                EventBus = this,
                QueueList = new List<QueueInfo>(),
                Config = consumerConfig == null || consumerConfig.MinQos == 0 ? DefaultConsumerConfig : consumerConfig
            };
            foreach (var route in RouteList)
            {
                consumer.QueueList.Add(new QueueInfo { RoutingKey = route, Queue = $"{route}_{observerGroup}" });
            }
            Consumers.Add(consumer);
            return this;
        }
        public Task Enable()
        {
            return Container.Work(this);
        }
        public Task DefaultConsumer<PrimaryKey>()
        {
            foreach (var group in observerUnitContainer.GetUnit<PrimaryKey>(ProducerType).GetGroups())
            {
                CreateConsumer<PrimaryKey>(group);
            };
            return Enable();
        }
    }
}
