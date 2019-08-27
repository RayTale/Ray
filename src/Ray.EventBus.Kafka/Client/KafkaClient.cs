using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;
using Ray.Core.Serialization;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Ray.EventBus.Kafka
{
    public class KafkaClient : IKafkaClient
    {
        readonly DefaultObjectPool<PooledProducer> producerObjectPool;
        readonly ConcurrentDictionary<string, DefaultObjectPool<PooledConsumer>> consumerPoolDict = new ConcurrentDictionary<string, DefaultObjectPool<PooledConsumer>>();
        readonly ConsumerConfig consumerConfig;
        readonly RayKafkaOptions rayKafkaOptions;
        readonly ISerializer serializer;
        readonly ILogger<KafkaClient> logger;
        public KafkaClient(
            IOptions<ProducerConfig> producerConfig,
            IOptions<ConsumerConfig> consumerConfig,
            IOptions<RayKafkaOptions> options,
            ILogger<KafkaClient> logger,
            ISerializer serializer)
        {
            producerObjectPool = new DefaultObjectPool<PooledProducer>(new ProducerPooledObjectPolicy(producerConfig.Value, logger), options.Value.ProducerMaxPoolSize);
            this.consumerConfig = consumerConfig.Value;
            this.consumerConfig.EnableAutoCommit = false;
            this.consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            this.consumerConfig.EnablePartitionEof = true;
            this.serializer = serializer;
            rayKafkaOptions = options.Value;
            this.logger = logger;
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
                 var configDict = serializer.Deserialize<IEnumerable<KeyValuePair<string, string>>>(serializer.Serialize(consumerConfig));
                 var config = new ConsumerConfig(configDict)
                 {
                     GroupId = group
                 };
                 return new DefaultObjectPool<PooledConsumer>(new ConsumerPooledObjectPolicy(config, logger), rayKafkaOptions.ConsumerMaxPoolSize);
             });
            var result = consumerObjectPool.Get();
            if (result.Pool == default)
                result.Pool = consumerObjectPool;
            return result;
        }
    }
}
