using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;
using System;

namespace Ray.EventBus.Kafka
{
    public class PooledConsumer : IDisposable
    {
        public DefaultObjectPool<PooledConsumer> Pool { get; set; }
        public IConsumer<string, byte[]> Handler { get; set; }

        public void Dispose()
        {
            Pool.Return(this);
        }
    }
}
