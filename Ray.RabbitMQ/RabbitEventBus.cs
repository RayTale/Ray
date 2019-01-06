using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.Utils;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitEventBus<W> where W : IBytesWrapper
    {
        private readonly ConsistentHash _CHash;
        public RabbitEventBus(
            IServiceProvider serviceProvider,
            IRabbitEventBusContainer<W> eventBusContainer,
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
        public IRabbitEventBusContainer<W> Container { get; }
        public string Exchange { get; }
        public string RoutePrefix { get; }
        public int LBCount { get; }
        public List<string> RouteList { get; }
        public List<Type> Producers { get; set; } = new List<Type>();
        public List<RabbitConsumer<W>> Consumers { get; set; } = new List<RabbitConsumer<W>>();
        public string GetRoute(string key)
        {
            return LBCount == 1 ? RoutePrefix : _CHash.GetNode(key); ;
        }
        public RabbitEventBus<W> BindProducer<T>()
        {
            Producers.Add(typeof(T));
            return this;
        }
        public RabbitConsumer<W> CreateConsumer<K>(string prefix = null, ushort minQos = 100, ushort incQos =100, ushort maxQos = 300, bool autoAck = false, bool errorReject = false)
        {
            var consumer = new RabbitConsumer<W>(ServiceProvider.GetService<ISerializer>())
            {
                EventBus = this,
                QueueList = new List<QueueInfo>(),
                AutoAck = autoAck,
                MaxQos = maxQos,
                MinQos = minQos,
                IncQos = incQos,
                ErrorReject = errorReject,
                HandlerFuncs = new List<Func<byte[], object, Task>>()
            };
            foreach (var route in RouteList)
            {
                consumer.QueueList.Add(new QueueInfo { RoutingKey = route, Queue = $"{prefix}_{route}" });
            }
            Consumers.Add(consumer);
            return consumer;
        }
        public Task Enable()
        {
            return Container.Work(this);
        }
    }
}
