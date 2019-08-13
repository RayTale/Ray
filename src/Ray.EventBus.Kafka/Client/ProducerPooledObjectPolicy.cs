using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace Ray.EventBus.Kafka
{
    public class ProducerPooledObjectPolicy : IPooledObjectPolicy<PooledProducer>
    {
        readonly ProducerConfig producerConfig;
        public ProducerPooledObjectPolicy(ProducerConfig producerConfig)
        {
            this.producerConfig = producerConfig;
            this.producerConfig.EnableDeliveryReports = false;
        }
        public PooledProducer Create()
        {
            return new PooledProducer
            {
                Handler = new ProducerBuilder<string, byte[]>(producerConfig).Build()
            };
        }

        public bool Return(PooledProducer obj)
        {
            return true;
        }
    }
}
