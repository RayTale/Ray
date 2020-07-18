using System;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace Ray.EventBus.Kafka
{
    public class PooledProducer : IDisposable
    {
        public DefaultObjectPool<PooledProducer> Pool { get; set; }

        public IProducer<string, byte[]> Handler { get; set; }

        public void Dispose()
        {
            this.Pool.Return(this);
        }
    }
}
