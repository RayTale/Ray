using Ray.Core.Abstractions;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.Utils;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public class KafkaEventBus
    {
        private readonly ConsistentHash _CHash;
        readonly IObserverUnitContainer observerUnitContainer;
        public KafkaEventBus(
            IObserverUnitContainer observerUnitContainer,
            IKafkaEventBusContainer eventBusContainer,
            string topic,
            int lBCount = 1,
            int retryCount = 3,
            int retryIntervals = 500)
        {
            if (string.IsNullOrEmpty(topic))
                throw new ArgumentNullException(nameof(topic));
            if (lBCount < 1)
                throw new ArgumentOutOfRangeException($"{nameof(lBCount)} must be greater than 1");
            this.observerUnitContainer = observerUnitContainer;
            Container = eventBusContainer;
            Topic = topic;
            LBCount = lBCount;
            ConsumerOptions = new ConsumerOptions
            {
                RetryCount = retryCount,
                RetryIntervals = retryIntervals
            };
            Topics = new List<string>();
            if (LBCount == 1)
            {
                Topics.Add(Topic);
            }
            else
            {
                for (int i = 0; i < LBCount; i++)
                {
                    Topics.Add($"{Topic }_{ i}");
                }
            }
            _CHash = new ConsistentHash(Topics, lBCount * 10);
        }
        public IKafkaEventBusContainer Container { get; }
        public string Topic { get; }
        public int LBCount { get; }
        public List<string> Topics { get; }
        public Type ProducerType { get; set; }
        public ConsumerOptions ConsumerOptions { get; set; }
        public List<KafkaConsumer> Consumers { get; set; } = new List<KafkaConsumer>();
        public string GetRoute(string key)
        {
            return LBCount == 1 ? Topic : _CHash.GetNode(key); ;
        }
        public KafkaEventBus BindProducer<TGrain>()
        {
            return BindProducer(typeof(TGrain));
        }
        public KafkaEventBus BindProducer(Type grainType)
        {
            if (ProducerType == null)
                ProducerType = grainType;
            else
                throw new EventBusRepeatBindingProducerException(grainType.FullName);
            return this;
        }
        public KafkaEventBus AddGrainConsumer<PrimaryKey>(string observerGroup)
        {
            var observerUnit = observerUnitContainer.GetUnit<PrimaryKey>(ProducerType);
            var consumer = new KafkaConsumer(
               observerUnit.GetEventHandlers(observerGroup),
               observerUnit.GetBatchEventHandlers(observerGroup))
            {
                EventBus = this,
                Topics = Topics,
                Group = observerGroup,
                Config = ConsumerOptions
            };
            Consumers.Add(consumer);
            return this;
        }
        public KafkaEventBus AddConsumer(
            Func<BytesBox, Task> handler,
            Func<List<BytesBox>, Task> batchHandler,
            string observerGroup)
        {
            var consumer = new KafkaConsumer(
                new List<Func<BytesBox, Task>> { handler },
                new List<Func<List<BytesBox>, Task>> { batchHandler })
            {
                EventBus = this,
                Topics = Topics,
                Group = observerGroup,
                Config = ConsumerOptions
            };
            Consumers.Add(consumer);
            return this;
        }
        public Task Enable()
        {
            return Container.Work(this);
        }
        public Task AddGrainConsumer<PrimaryKey>()
        {
            foreach (var group in observerUnitContainer.GetUnit<PrimaryKey>(ProducerType).GetGroups())
            {
                AddGrainConsumer<PrimaryKey>(group);
            };
            return Enable();
        }
    }
}
