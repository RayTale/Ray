using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;
using System;

namespace Ray.EventBus.Kafka
{
    public class PooledConsumer : IDisposable
    {
        public DefaultObjectPool<PooledConsumer> Pool { get; set; }
        public IConsumer<string, byte[]> Handler { get; set; }
        /// <summary>
        /// 消费者批量处理每次处理的消息量
        /// </summary>
        public int MaxBatchSize { get; set; }
        /// <summary>
        /// 消费者批量处理每次处理的最大延时
        /// </summary>
        public int MaxMillisecondsInterval { get; set; }
        public void Dispose()
        {
            Pool.Return(this);
        }
    }
}
