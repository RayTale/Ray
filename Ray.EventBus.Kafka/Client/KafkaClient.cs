using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;
using Ray.Core.Serialization;
using System.Collections.Concurrent;

namespace Ray.EventBus.Kafka
{
    public class KafkaClient : IKafkaClient
    {
        readonly DefaultObjectPool<PooledProducer> producerObjectPool;
        readonly ConcurrentDictionary<string, DefaultObjectPool<PooledConsumer>> consumerPoolDict = new ConcurrentDictionary<string, DefaultObjectPool<PooledConsumer>>();
        readonly ConsumerConfig consumerConfig;
        readonly RayKafkaOptions rayKafkaOptions;
        readonly ISerializer serializer;
        public KafkaClient(
            IOptions<ProducerConfig> producerConfig,
             IOptions<ConsumerConfig> consumerConfig,
            IOptions<RayKafkaOptions> options,
            ISerializer serializer)
        {
            producerObjectPool = new DefaultObjectPool<PooledProducer>(new ProducerPooledObjectPolicy(producerConfig.Value), options.Value.ProducerMaxPoolSize);
            this.consumerConfig = consumerConfig.Value;
            this.serializer = serializer;
            rayKafkaOptions = options.Value;
        }
        public PooledProducer GetProducer()
        {
            var result = producerObjectPool.Get();
            if (result.Pool == default)
                result.Pool = producerObjectPool;
            return result;
        }
        public PooledConsumer GetConsumer(string group)
        {
            var consumerObjectPool = consumerPoolDict.GetOrAdd(group, key =>
             {
                 var config = serializer.Deserialize<ConsumerConfig>(serializer.SerializeToBytes(consumerConfig));
                 config.GroupId = group;
                 return new DefaultObjectPool<PooledConsumer>(new ConsumerPooledObjectPolicy(config), rayKafkaOptions.ConsumerMaxPoolSize);
             });
            var result = consumerObjectPool.Get();
            if (result.Pool == default)
                result.Pool = consumerObjectPool;
            return result;
        }
    }
}
