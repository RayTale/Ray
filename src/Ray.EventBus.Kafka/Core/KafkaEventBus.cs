using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.Utils;

namespace Ray.EventBus.Kafka
{
    public class KafkaEventBus
    {
        private readonly ConsistentHash CHash;
        private readonly IObserverUnitContainer observerUnitContainer;

        public KafkaEventBus(
            IObserverUnitContainer observerUnitContainer,
            IKafkaEventBusContainer eventBusContainer,
            string topic,
            int lBCount = 1,
            int retryCount = 3,
            int retryIntervals = 500)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentNullException(nameof(topic));
            }

            if (lBCount < 1)
            {
                throw new ArgumentOutOfRangeException($"{nameof(lBCount)} must be greater than 1");
            }

            this.observerUnitContainer = observerUnitContainer;
            this.Container = eventBusContainer;
            this.Topic = topic;
            this.LBCount = lBCount;
            this.ConsumerOptions = new ConsumerOptions
            {
                RetryCount = retryCount,
                RetryIntervals = retryIntervals
            };
            this.Topics = new List<string>();
            if (this.LBCount == 1)
            {
                this.Topics.Add(this.Topic);
            }
            else
            {
                for (int i = 0; i < this.LBCount; i++)
                {
                    this.Topics.Add($"{this.Topic}_{i}");
                }
            }

            this.CHash = new ConsistentHash(this.Topics, lBCount * 10);
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
            return this.LBCount == 1 ? this.Topic : this.CHash.GetNode(key);
        }

        public KafkaEventBus BindProducer<TGrain>()
        {
            return this.BindProducer(typeof(TGrain));
        }

        public KafkaEventBus BindProducer(Type grainType)
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

        public KafkaEventBus AddGrainConsumer<PrimaryKey>(string observerGroup)
        {
            var observerUnit = this.observerUnitContainer.GetUnit<PrimaryKey>(this.ProducerType);
            var consumer = new KafkaConsumer(
               observerUnit.GetEventHandlers(observerGroup),
               observerUnit.GetBatchEventHandlers(observerGroup))
            {
                EventBus = this,
                Topics = this.Topics,
                Group = observerGroup,
                Config = this.ConsumerOptions
            };
            this.Consumers.Add(consumer);
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
                Topics = this.Topics,
                Group = observerGroup,
                Config = this.ConsumerOptions
            };
            this.Consumers.Add(consumer);
            return this;
        }

        public Task Enable()
        {
            return this.Container.Work(this);
        }

        public Task AddGrainConsumer<PrimaryKey>()
        {
            foreach (var group in this.observerUnitContainer.GetUnit<PrimaryKey>(this.ProducerType).GetGroups())
            {
                this.AddGrainConsumer<PrimaryKey>(group);
            }

            return this.Enable();
        }
    }
}
