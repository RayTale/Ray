using System.Threading.Tasks;
using Confluent.Kafka;
using Ray.Core;
using Ray.Core.EventBus;

namespace Ray.EventBus.Kafka
{
    /// <summary>
    /// Message producer
    /// </summary>
    public class KafkaProducer : IProducer
    {
        private readonly KafkaEventBus publisher;
        private readonly IKafkaClient client;

        public KafkaProducer(
            IKafkaClient client,
            KafkaEventBus publisher)
        {
            this.publisher = publisher;
            this.client = client;
        }

        public ValueTask Publish(byte[] bytes, string hashKey)
        {
            var topic = this.publisher.GetRoute(hashKey);
            using var producer = this.client.GetProducer();
            producer.Handler.Produce(topic, new Message<string, byte[]> { Key = hashKey, Value = bytes });
            return Consts.ValueTaskDone;
        }
    }
}
