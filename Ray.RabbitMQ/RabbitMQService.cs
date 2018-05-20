using System.Threading.Tasks;
using Ray.Core.MQ;

namespace Ray.RabbitMQ
{
    public class RabbitMQService : IMQService
    {
        RabbitPubAttribute publisher;
        public RabbitMQService(RabbitPubAttribute rabbitMQInfo) => publisher = rabbitMQInfo;

        public Task Publish(byte[] bytes, string hashKey)
        {
            publisher.Publish(bytes, hashKey, false);
            return Task.CompletedTask;
        }
    }
}
