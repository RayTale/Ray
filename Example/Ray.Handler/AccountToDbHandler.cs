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
    public sealed class AccountToDbHandler : SubHandler<MessageInfo>
    {
        IOrleansClientFactory clientFactory;
        public AccountToDbHandler(IServiceProvider svProvider, IOrleansClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        public override Task Tell(byte[] bytes, IMessage data, MessageInfo msg)
        {
            if(data is IActorOwnMessage<string> actorData)
                return clientFactory.GetClient().GetGrain<IAccountDb>(actorData.StateId).Tell(bytes);
            return Task.CompletedTask;
        }
    }
}
