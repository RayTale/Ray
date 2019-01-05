using System.Threading.Tasks;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitMQService : IProducer
    {
        readonly RabbitPublisher publisher;
        public RabbitMQService(RabbitPublisher publisher) => this.publisher = publisher;

        public async ValueTask Publish(byte[] bytes, string hashKey)
        {
            var task = publisher.GetQueue(hashKey);
            if (!task.IsCompleted)
                await task;
            var (queue, model) = task.Result;
            model.Publish(bytes, publisher.Exchange, queue, false);
        }
    }
}
