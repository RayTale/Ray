using Ray.Core;
using Ray.Core.Message;
using Ray.Core.MQ;
using Ray.RabbitMQ;
using System.Threading.Tasks;
using Ray.IGrains.ToReadActors;
using Ray.IGrains;

namespace Ray.Handler
{
    [RabbitSub("Read", "Account", "account")]
    public class AccountToReadHandler : AllSubHandler<string, MessageInfo>
    {
        public override Task Tell(byte[] dataBytes, IActorOwnMessage<string> data, MessageInfo msg)
        {
            var grain = HandlerStart.Client.GetGrain<IAccountToRead>(data.StateId);
            return grain.Tell(dataBytes);
        }
    }
}
