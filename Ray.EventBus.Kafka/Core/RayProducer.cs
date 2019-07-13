using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;
using System;

namespace Ray.EventBus.Kafka
{
    public class RayProducer : IDisposable
    {
        public DefaultObjectPool<RayProducer> Pool { get; set; }
        public IProducer<string, byte[]> Handler { get; set; }

        public void Dispose()
        {
            Pool.Return(this);
        }
    }
}
