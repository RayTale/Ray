using Ray.Core;
using Ray.Core.EventBus;
using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitProducer : IProducer
    {
        readonly RabbitEventBus publisher;
        readonly IRabbitMQClient rabbitMQClient;
        public RabbitProducer(
            IRabbitMQClient rabbitMQClient,
            RabbitEventBus publisher)
        {
            this.publisher = publisher;
            this.rabbitMQClient = rabbitMQClient;
        }
        public ValueTask Publish(byte[] bytes, string hashKey)
        {
            using var model = rabbitMQClient.PullModel();
            model.Publish(bytes, publisher.Exchange, publisher.GetRoute(hashKey), publisher.Persistent);
            return Consts.ValueTaskDone;
        }
    }
}
