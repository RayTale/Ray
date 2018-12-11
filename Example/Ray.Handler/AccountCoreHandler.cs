using Ray.Core;
using Ray.Core.Internal;
using Ray.Core.EventBus;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.RabbitMQ;
using System;
using System.Threading.Tasks;

namespace Ray.Handler
{
    public sealed class AccountCoreHandler : MulSubtHandler<long, MessageInfo>
    {
        readonly IClientFactory clientFactory;
        public AccountCoreHandler(IServiceProvider svProvider, IClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        protected override Task SendToAsyncGrain(byte[] bytes, IEventBase<long> evt)
        {
            var client = clientFactory.GetClient();
            return client.GetGrain<IAccountFlow>(evt.StateId).ConcurrentTell(bytes);
        }
    }
}
