﻿using Confluent.Kafka;
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
        readonly RayKafkaOptions options;
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
            this.options = options.Value;
            this.logger = logger;
        }
        public PooledProducer GetProducer()
        {
            var result = producerObjectPool.Get();
            if (result.Pool is null)
                result.Pool = producerObjectPool;
            return result;
        }
        public PooledConsumer GetConsumer(string group)
        {
            var consumerObjectPool = consumerPoolDict.GetOrAdd(group, key =>
             {
                 var kvList = serializer.Deserialize<List<KeyValuePair<string, string>>>(serializer.SerializeToUtf8Bytes(consumerConfig));
                 var dict = new Dictionary<string, string>(kvList);
                 var config = new ConsumerConfig(dict)
                 {
                     GroupId = group
                 };
                 return new DefaultObjectPool<PooledConsumer>(new ConsumerPooledObjectPolicy(config, logger), options.ConsumerMaxPoolSize);
             });
            var result = consumerObjectPool.Get();
            if (result.Pool is null)
            {
                result.Pool = consumerObjectPool;
                result.MaxBatchSize = options.CunsumerMaxBatchSize;
                result.MaxMillisecondsInterval = options.CunsumerMaxMillisecondsInterval;
            }
            return result;
        }
    }
}
