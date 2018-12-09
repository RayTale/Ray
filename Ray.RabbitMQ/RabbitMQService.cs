using System.Threading.Tasks;
using Ray.Core.MQ;

namespace Ray.RabbitMQ
{
    public class RabbitMQService : IMQService
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
