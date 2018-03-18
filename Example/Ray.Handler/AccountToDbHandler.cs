using Ray.Core.Message;
using Ray.RabbitMQ;
using System.Threading.Tasks;
using Ray.IGrains;
using System;
using Ray.Core.MQ;
using Ray.Core;
using Ray.IGrains.Actors;

namespace Ray.Handler
{
    [RabbitSub("Read", "Account", "account")]
    public sealed class AccountToDbHandler : SubHandler<string, MessageInfo>
    {
        IOrleansClientFactory clientFactory;
        public AccountToDbHandler(IServiceProvider svProvider, IOrleansClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }
        public override Task Tell(byte[] bytes, IActorOwnMessage<string> data, MessageInfo msg)
        {
            return clientFactory.GetClient().GetGrain<IAccountDb>(data.StateId).Tell(bytes);
        }
    }
}
