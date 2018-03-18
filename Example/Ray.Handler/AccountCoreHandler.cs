using Ray.Core;
using Ray.Core.Message;
using Ray.Core.MQ;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.RabbitMQ;
using System;
using System.Threading.Tasks;

namespace Ray.Handler
{
    [RabbitSub("Core", "Account", "account")]
    public sealed class AccountCoreHandler : SubHandler<string, MessageInfo>
    {
        IOrleansClientFactory clientFactory;
        public AccountCoreHandler(IServiceProvider svProvider, IOrleansClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }
        public override Task Tell(byte[] bytes, IActorOwnMessage<string> data, MessageInfo msg)
        {
            var client = clientFactory.GetClient();
            return Task.WhenAll(
                client.GetGrain<IAccountRep>(data.StateId).Tell(bytes),
                client.GetGrain<IAccountFlow>(data.StateId).Tell(bytes)
                );
        }
    }
}
