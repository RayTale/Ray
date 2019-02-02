using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.Exceptions;
using Ray.Core.Utils;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitEventBus
    {
        private readonly ConsistentHash _CHash;
        public RabbitEventBus(
            IServiceProvider serviceProvider,
            IRabbitEventBusContainer eventBusContainer,
            string exchange, string routePrefix, int lBCount = 1)
        {
            if (string.IsNullOrEmpty(exchange))
                throw new ArgumentNullException(nameof(exchange));
            if (string.IsNullOrEmpty(routePrefix))
                throw new ArgumentNullException(nameof(routePrefix));
            if (lBCount < 1)
                throw new ArgumentOutOfRangeException($"{nameof(lBCount)} must be greater than 1");
            ServiceProvider = serviceProvider;
            Container = eventBusContainer;
            Exchange = exchange;
            RoutePrefix = routePrefix;
            LBCount = lBCount;
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
        public List<string> RouteList { get; }
        public Type ProducerType { get; set; }
        public List<RabbitConsumer> Consumers { get; set; } = new List<RabbitConsumer>();
        public string GetRoute(string key)
        {
            return LBCount == 1 ? RoutePrefix : _CHash.GetNode(key); ;
        }
        public RabbitEventBus BindProducer<T>()
        {
            if (ProducerType == null)
                ProducerType = typeof(T);
            else
                throw new EventBusMultiplebindingProducerException(typeof(T).FullName);
            return this;
        }
        public RabbitEventBus CreateConsumer<K>(string prefix = null, ushort minQos = 100, ushort incQos = 100, ushort maxQos = 300, bool autoAck = false, bool errorReject = false)
        {
            var consumer = new RabbitConsumer(ServiceProvider.GetService<IFollowUnitContainer>().GetUnit<K>(ProducerType).GetEventHandlers())
            {
                EventBus = this,
                QueueList = new List<QueueInfo>(),
                AutoAck = autoAck,
                MaxQos = maxQos,
                MinQos = minQos,
                IncQos = incQos,
                ErrorReject = errorReject
            };
            foreach (var route in RouteList)
            {
                consumer.QueueList.Add(new QueueInfo { RoutingKey = route, Queue = $"{prefix}_{route}" });
            }
            Consumers.Add(consumer);
            return this;
        }
        public Task Enable()
        {
            return Container.Work(this);
        }
        public Task DefaultConsumer<K>()
        {
            return CreateConsumer<K>(DefaultPrefix.primary).CreateConsumer<K>(DefaultPrefix.secondary).Enable();
        }
    }
}
