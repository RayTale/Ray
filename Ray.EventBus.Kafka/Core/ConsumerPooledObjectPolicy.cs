using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace Ray.EventBus.Kafka
{
    public class ConsumerPooledObjectPolicy : IPooledObjectPolicy<RayConsumer>
    {
        readonly ConsumerConfig consumerConfig;
        public ConsumerPooledObjectPolicy(ConsumerConfig consumerConfig)
        {
            this.consumerConfig = consumerConfig;
        }
        public RayConsumer Create()
        {
            return new RayConsumer
            {
                Handler = new ConsumerBuilder<string, byte[]>(consumerConfig).Build()
            };
        }

        public bool Return(RayConsumer obj)
        {
            return true;
        }
    }
}
