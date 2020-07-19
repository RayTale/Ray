using System.Collections.Concurrent;
using System.Collections.Generic;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;
using Ray.Core.Serialization;

namespace Ray.EventBus.Kafka
{
    public class KafkaClient : IKafkaClient
    {
        private readonly DefaultObjectPool<PooledProducer> producerObjectPool;
        private readonly ConcurrentDictionary<string, DefaultObjectPool<PooledConsumer>> consumerPoolDict = new ConcurrentDictionary<string, DefaultObjectPool<PooledConsumer>>();
        private readonly ConsumerConfig consumerConfig;
        private readonly RayKafkaOptions options;
        private readonly ISerializer serializer;
        private readonly ILogger<KafkaClient> logger;

        public KafkaClient(
            IOptions<ProducerConfig> producerConfig,
            IOptions<ConsumerConfig> consumerConfig,
            IOptions<RayKafkaOptions> options,
            ILogger<KafkaClient> logger,
            ISerializer serializer)
        {
            this.producerObjectPool = new DefaultObjectPool<PooledProducer>(new ProducerPooledObjectPolicy(producerConfig.Value, logger), options.Value.ProducerMaxPoolSize);
            this.consumerConfig = consumerConfig.Value;
            this.consumerConfig.EnableAutoCommit = false;
            this.consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            this.consumerConfig.EnablePartitionEof = true;
            this.serializer = serializer;
            this.options = options.Value;
            this.logger = logger;
        }

        public PooledProducer GetProducer()
        {
            var result = this.producerObjectPool.Get();
            if (result.Pool is null)
            {
                result.Pool = this.producerObjectPool;
            }

            return result;
        }

        public PooledConsumer GetConsumer(string group)
        {
            var consumerObjectPool = this.consumerPoolDict.GetOrAdd(group, key =>
             {
                 var kvList = this.serializer.Deserialize<List<KeyValuePair<string, string>>>(this.serializer.SerializeToUtf8Bytes(this.consumerConfig));
                 var dict = new Dictionary<string, string>(kvList);
                 var config = new ConsumerConfig(dict)
                 {
                     GroupId = group
                 };
                 return new DefaultObjectPool<PooledConsumer>(new ConsumerPooledObjectPolicy(config, this.logger), this.options.ConsumerMaxPoolSize);
             });
            var result = consumerObjectPool.Get();
            if (result.Pool is null)
            {
                result.Pool = consumerObjectPool;
                result.MaxBatchSize = this.options.CunsumerMaxBatchSize;
                result.MaxMillisecondsInterval = this.options.CunsumerMaxMillisecondsInterval;
            }

            return result;
        }
    }
}
