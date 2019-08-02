using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace Ray.EventBus.Kafka
{
    public class ConsumerPooledObjectPolicy : IPooledObjectPolicy<PooledConsumer>
    {
        readonly ConsumerConfig consumerConfig;
        public ConsumerPooledObjectPolicy(ConsumerConfig consumerConfig)
        {
            this.consumerConfig = consumerConfig;
        }
        public PooledConsumer Create()
        {
            return new PooledConsumer
            {
                Handler = new ConsumerBuilder<string, byte[]>(consumerConfig).Build()
            };
        }

        public bool Return(PooledConsumer obj)
        {
            return true;
        }
    }
}
