using System.Threading.Tasks;
using Ray.Core;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitProducer : IProducer
    {
        private readonly RabbitEventBus publisher;
        private readonly IRabbitMQClient rabbitMQClient;

        public RabbitProducer(
            IRabbitMQClient rabbitMQClient,
            RabbitEventBus publisher)
        {
            this.publisher = publisher;
            this.rabbitMQClient = rabbitMQClient;
        }

        public ValueTask Publish(byte[] bytes, string hashKey)
        {
            using var model = this.rabbitMQClient.PullModel();
            model.Publish(bytes, this.publisher.Exchange, this.publisher.GetRoute(hashKey), this.publisher.Persistent);
            return Consts.ValueTaskDone;
        }
    }
}
