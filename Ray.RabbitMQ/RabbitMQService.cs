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
        public RabbitMQService(RabbitPubAttribute rabbitMQInfo) => this.publisher = rabbitMQInfo;

        public async Task Send(IMessage msg, byte[] bytes, string hashKey)
        {
            var data = new W();
            data.TypeCode = msg.TypeCode;
            data.BinaryBytes = bytes;
            await publisher.Send(data, hashKey);
        }
    }
}
