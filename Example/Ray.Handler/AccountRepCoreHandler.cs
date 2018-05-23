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
    [RabbitSub("Rep", "Account", "account")]
    public sealed class AccountRepCoreHandler : MultHandler<long, MessageInfo>
    {
        IClientFactory clientFactory;
        public AccountRepCoreHandler(IServiceProvider svProvider, IClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        protected override Task SendToAsyncGrain(byte[] bytes, IEventBase<long> evt)
        {
            var client = clientFactory.CreateClient();
            return client.GetGrain<IAccountRep>(evt.StateId).Tell(bytes);
        }
    }
}
