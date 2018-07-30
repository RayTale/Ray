using System.Threading.Tasks;
using Ray.Core.MQ;

namespace Ray.RabbitMQ
{
    public class RabbitMQService : IMQService
    {
        RabbitPubAttribute publisher;
        public RabbitMQService(RabbitPubAttribute rabbitMQInfo) => publisher = rabbitMQInfo;

        public void Publish(byte[] bytes, string hashKey)
        {
            var (queue, model) = publisher.GetQueue(hashKey);
            model.Publish(bytes, publisher.Exchange, queue, false);
        }
    }
}
