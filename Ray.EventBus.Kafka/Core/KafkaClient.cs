using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;

namespace Ray.EventBus.Kafka
{
    public class KafkaClient
    {
        readonly DefaultObjectPool<RayProducer> producerObjectPool;
        readonly DefaultObjectPool<RayConsumer> consumerObjectPool;
        public KafkaClient(
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            IOptions<RayKafkaOptions> options)
        {
            producerObjectPool = new DefaultObjectPool<RayProducer>(new ProducerPooledObjectPolicy(producerConfig), options.Value.ProducerMaxPoolSize);
            consumerObjectPool = new DefaultObjectPool<RayConsumer>(new ConsumerPooledObjectPolicy(consumerConfig), options.Value.ConsumerMaxPoolSize);
        }
        public RayProducer GetProducer()
        {
            var result = producerObjectPool.Get();
            if (result.Pool == default)
                result.Pool = producerObjectPool;
            return result;
        }
        public RayConsumer GetConsumer()
        {
            var result = consumerObjectPool.Get();
            if (result.Pool == default)
                result.Pool = consumerObjectPool;
            return result;
        }
    }
}
