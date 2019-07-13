using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace Ray.EventBus.Kafka
{
    public class RayConsumer
    {
        public DefaultObjectPool<RayConsumer> Pool { get; set; }
        public IConsumer<string, byte[]> Handler { get; set; }

        public void Dispose()
        {
            Pool.Return(this);
        }
    }
}
