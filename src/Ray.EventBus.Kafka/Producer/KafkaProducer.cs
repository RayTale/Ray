using Confluent.Kafka;
using Ray.Core.EventBus;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    /// <summary>
    /// Message producer
    /// </summary>
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
        public ValueTask Publish(byte[] bytes, string hashKey)
        {
            var topic = publisher.GetRoute(hashKey);
            using (var producer = client.GetProducer())
            {
                producer.Handler.Produce(topic, new Message<string, byte[]> { Key = hashKey, Value = bytes });
            }
            return new ValueTask(Task.CompletedTask);

        }
    }
}
