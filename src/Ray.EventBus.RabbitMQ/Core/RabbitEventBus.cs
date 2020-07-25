using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.Utils;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitEventBus
    {
        private readonly ConsistentHash CHash;
        private readonly IObserverUnitContainer observerUnitContainer;

        public RabbitEventBus(
            IObserverUnitContainer observerUnitContainer,
            IRabbitEventBusContainer eventBusContainer,
            string exchange, string routePrefix, int lBCount = 1, bool autoAck = false, bool persistent = false, int retryCount = 3, int retryIntervals = 500)
        {
            if (string.IsNullOrEmpty(exchange))
            {
                throw new ArgumentNullException(nameof(exchange));
            }

            if (string.IsNullOrEmpty(routePrefix))
            {
                throw new ArgumentNullException(nameof(routePrefix));
            }

            if (lBCount < 1)
            {
                throw new ArgumentOutOfRangeException($"{nameof(lBCount)} must be greater than 1");
            }

            this.observerUnitContainer = observerUnitContainer;
            this.Container = eventBusContainer;
            this.Exchange = exchange;
            this.RoutePrefix = routePrefix;
            this.LBCount = lBCount;
            this.Persistent = persistent;
            this.ConsumerConfig = new ConsumerOptions
            {
                AutoAck = autoAck,
                RetryCount = retryCount,
                RetryIntervals = retryIntervals
            };
            this.RouteList = new List<string>();
            if (this.LBCount == 1)
            {
                this.RouteList.Add(routePrefix);
            }
            else
            {
                for (int i = 0; i < this.LBCount; i++)
                {
                    this.RouteList.Add($"{routePrefix}_{i}");
                }
            }

            this.CHash = new ConsistentHash(this.RouteList, lBCount * 10);
        }

        public IRabbitEventBusContainer Container { get; }

        public string Exchange { get; }

        public string RoutePrefix { get; }

        public int LBCount { get; }

        public ConsumerOptions ConsumerConfig { get; set; }

        public List<string> RouteList { get; }

        public Type ProducerType { get; set; }

        /// <summary>
        /// 消息是否持久化
        /// </summary>
        public bool Persistent { get; set; }

        public List<RabbitConsumer> Consumers { get; set; } = new List<RabbitConsumer>();

        public string GetRoute(string key)
        {
            return this.LBCount == 1 ? this.RoutePrefix : this.CHash.GetNode(key);
        }

        public RabbitEventBus BindProducer<TGrain>()
        {
            return this.BindProducer(typeof(TGrain));
        }

        public RabbitEventBus BindProducer(Type grainType)
        {
            if (this.ProducerType == null)
            {
                this.ProducerType = grainType;
            }
            else
            {
                throw new EventBusRepeatBindingProducerException(grainType.FullName);
            }

            return this;
        }

        public RabbitEventBus AddGrainConsumer<PrimaryKey>(string observerGroup)
        {
            var observerUnit = this.observerUnitContainer.GetUnit<PrimaryKey>(this.ProducerType);
            var consumer = new RabbitConsumer(
                observerUnit.GetEventHandlers(observerGroup),
                observerUnit.GetBatchEventHandlers(observerGroup))
            {
                EventBus = this,
                QueueList = this.RouteList.Select(route => new QueueInfo { RoutingKey = route, Queue = $"{route}_{observerGroup}" }).ToList(),
                Config = this.ConsumerConfig
            };
            this.Consumers.Add(consumer);
            return this;
        }

        public RabbitEventBus AddConsumer(
            Func<BytesBox, Task> handler,
            Func<List<BytesBox>, Task> batchHandler,
            string observerGroup)
        {
            var consumer = new RabbitConsumer(
                new List<Func<BytesBox, Task>> { handler },
                new List<Func<List<BytesBox>, Task>> { batchHandler })
            {
                EventBus = this,
                QueueList = this.RouteList.Select(route => new QueueInfo { RoutingKey = route, Queue = $"{route}_{observerGroup}" }).ToList(),
                Config = this.ConsumerConfig
            };
            this.Consumers.Add(consumer);
            return this;
        }

        public Task Enable()
        {
            return this.Container.Work(this);
        }

        public Task AddGrainConsumer<TPrimaryKey>()
        {
            foreach (var group in this.observerUnitContainer.GetUnit<TPrimaryKey>(this.ProducerType).GetGroups())
            {
                this.AddGrainConsumer<TPrimaryKey>(group);
            }

            return this.Enable();
        }
    }
}
