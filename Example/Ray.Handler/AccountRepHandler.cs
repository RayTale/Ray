using Ray.Core;
using Ray.Core.Internal;
using Ray.Core.EventBus;
using Ray.IGrains;
using Ray.IGrains.Actors;
using System;
using System.Threading.Tasks;

namespace Ray.Handler
{
    public sealed class AccountRepHandler : MulSubtHandler<long, MessageInfo>
    {
        readonly IClientFactory clientFactory;
        public AccountRepHandler(IServiceProvider svProvider, IClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        protected override Task SendToAsyncGrain(byte[] bytes, IEventBase<long> evt)
        {
            var client = clientFactory.GetClient();
            return client.GetGrain<IAccountRep>(evt.StateId).Tell(bytes);
        }
    }
}
