using Ray.Core.Abstractions;
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
            bool reenqueue = true)
        {
            if (string.IsNullOrEmpty(topic))
                throw new ArgumentNullException(nameof(topic));
            if (lBCount < 1)
                throw new ArgumentOutOfRangeException($"{nameof(lBCount)} must be greater than 1");
            this.observerUnitContainer = observerUnitContainer;
            Container = eventBusContainer;
            Topic = topic;
            LBCount = lBCount;
            Reenqueue = reenqueue;
            Topics = new List<string>();
            if (LBCount == 1)
            {
                Topics.Add(Topic);
            }
            else
            {
                for (int i = 0; i < LBCount; i++)
                {
                    Topics.Add($"{Topic }_{ i.ToString()}");
                }
            }
            _CHash = new ConsistentHash(Topics, lBCount * 10);
        }
        public IKafkaEventBusContainer Container { get; }
        public string Topic { get; }
        public int LBCount { get; }
        public List<string> Topics { get; }
        public bool Reenqueue { get; set; }
        public Type ProducerType { get; set; }
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
            var consumer = new KafkaConsumer(observerUnitContainer.GetUnit<PrimaryKey>(ProducerType).GetEventHandlers(observerGroup))
            {
                EventBus = this,
                Topics = Topics,
                Group = observerGroup
            };
            Consumers.Add(consumer);
            return this;
        }
        public KafkaEventBus AddConsumer(Func<byte[], Task> handler, string observerGroup)
        {
            var consumer = new KafkaConsumer(new List<Func<byte[], Task>> { handler })
            {
                EventBus = this,
                Topics = Topics,
                Group = observerGroup
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
