using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;
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
        public RabbitConsumer<W> DefiningConsumer<K>(string prefix = null, ushort minQos = 150, ushort incQos = 20, ushort maxQos = 200, bool autoAck = false, bool errorReject = false)
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
        public Task Complete()
        {
            return Container.Work(this);
        }
    }
    public class RabbitConsumer<W> : Consumer<W>
        where W : IBytesWrapper
    {
        public RabbitConsumer(ISerializer serializer) : base(serializer)
        {
        }
        public RabbitEventBus<W> EventBus { get; set; }
        public List<QueueInfo> QueueList { get; set; }
        public ushort MinQos { get; set; }
        public ushort IncQos { get; set; }
        public ushort MaxQos { get; set; }
        public bool AutoAck { get; set; }
        public bool ErrorReject { get; set; }
        public List<Func<byte[], object, Task>> HandlerFuncs { get; set; }

        public override Task Handler(byte[] bytes, object data)
        {
            return Task.WhenAll(HandlerFuncs.Select(func => func(bytes, data)));
        }
        public RabbitConsumer<W> BindFollow<F>(Func<byte[], object, Task> handler)
            where F : IFollow
        {
            HandlerFuncs.Add(handler);
            return this;
        }
        public RabbitConsumer<W> BindFollowWithLongId<F>()
            where F : IFollow, IGrainWithIntegerKey
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEventBase<long> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.StateId).Tell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> BindConcurrentFollowWithLongId<F>()
            where F : IConcurrentFollow, IGrainWithIntegerKey
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEventBase<long> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.StateId).ConcurrentTell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> BindFollowWithStringId<F>()
            where F : IFollow, IGrainWithStringKey
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEventBase<string> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.StateId).Tell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> BindConcurrentFollowWithStringId<F>()
            where F : IConcurrentFollow, IGrainWithStringKey
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEventBase<string> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.StateId).ConcurrentTell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
    }
    public class QueueInfo
    {
        public string Queue { get; set; }
        public string RoutingKey { get; set; }
    }
}
