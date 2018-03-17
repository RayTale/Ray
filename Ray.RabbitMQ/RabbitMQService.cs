using System.Threading.Tasks;
using Ray.Core;
using Ray.Core.Message;
using Ray.Core.MQ;

namespace Ray.RabbitMQ
{
    public class RabbitMQService<W> : IMQService
        where W : MessageWrapper, new()
    {
        RabbitPubAttribute publisher;
        public RabbitMQService(RabbitPubAttribute rabbitMQInfo) => publisher = rabbitMQInfo;

        public Task Publish(IMessage msg, byte[] bytes, string hashKey)
        {
            var data = new W
            {
                TypeCode = msg.TypeCode,
                BinaryBytes = bytes
            };
            return publisher.Publish(data, hashKey);
        }
    }
}
