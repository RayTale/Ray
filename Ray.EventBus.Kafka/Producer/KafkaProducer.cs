using Confluent.Kafka;
using Ray.Core.EventBus;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public class KafkaProducer : IProducer
    {
        readonly KafkaEventBus publisher;
        readonly IKafkaClient client;
        public KafkaProducer(
            IKafkaClient client,
            KafkaEventBus publisher)
        {
            this.publisher = publisher;
            this.client = client;
        }
        public async ValueTask Publish(byte[] bytes, string hashKey)
        {
            var topic = publisher.GetRoute(hashKey);
            using (var producer = client.GetProducer())
            {
                await producer.Handler.ProduceAsync(topic, new Message<string, byte[]> { Key = hashKey, Value = bytes });
            }
        }
    }
}
