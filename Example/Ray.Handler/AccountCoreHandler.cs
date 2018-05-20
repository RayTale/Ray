using Ray.Core;
using Ray.Core.EventSourcing;
using Ray.Core.MQ;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.RabbitMQ;
using System;
using System.Threading.Tasks;

namespace Ray.Handler
{
    [RabbitSub("Core", "Account", "account")]
    public sealed class AccountCoreHandler : MultHandler<string, MessageInfo>
    {
        IOrleansClientFactory clientFactory;
        public AccountCoreHandler(IServiceProvider svProvider, IOrleansClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        protected override Task SendToAsyncGrain(byte[] bytes, IEventBase<string> evt)
        {
            var client = clientFactory.CreateClient();
            return Task.WhenAll(
                client.GetGrain<IAccountRep>(evt.StateId).Tell(bytes),
                client.GetGrain<IAccountFlow>(evt.StateId).Tell(bytes)
                );
        }
    }
}
