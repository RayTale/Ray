using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace Ray.EventBus.Kafka
{
    public class ProducerPooledObjectPolicy : IPooledObjectPolicy<RayProducer>
    {
        readonly ProducerConfig producerConfig;
        public ProducerPooledObjectPolicy(ProducerConfig producerConfig)
        {
            this.producerConfig = producerConfig;
        }
        public RayProducer Create()
        {
            return new RayProducer
            {
                Handler = new ProducerBuilder<string, byte[]>(producerConfig).Build()
            };
        }

        public bool Return(RayProducer obj)
        {
            return true;
        }
    }
}
