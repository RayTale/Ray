using System;
using System.Threading.Tasks;
using Ray.Core.Abstractions;
using Ray.Core.Client;
using Ray.Core.EventBus;
using Ray.IGrains;
using Ray.IGrains.Actors;

namespace Ray.Handler
{
    public sealed class AccountRepHandler : MultiSubtHandler<long, MessageInfo>
    {
        readonly IClientFactory clientFactory;
        public AccountRepHandler(IServiceProvider svProvider, IClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        protected override Task SendToAsyncGrain(byte[] bytes, IEventBase<long> evt)
        {
            var client = clientFactory.Create();
            return client.GetGrain<IAccountRep>(evt.StateId).Tell(bytes);
        }
    }
}
