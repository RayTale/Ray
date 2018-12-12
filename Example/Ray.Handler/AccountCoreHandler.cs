using System;
using System.Threading.Tasks;
using Ray.Core.Client;
using Ray.Core.EventBus;
using Ray.Core.Internal;
using Ray.IGrains;
using Ray.IGrains.Actors;

namespace Ray.Handler
{
    public sealed class AccountCoreHandler : MultiSubtHandler<long, MessageInfo>
    {
        readonly IClientFactory clientFactory;
        public AccountCoreHandler(IServiceProvider svProvider, IClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        protected override Task SendToAsyncGrain(byte[] bytes, IEventBase<long> evt)
        {
            var client = clientFactory.Create();
            return client.GetGrain<IAccountFlow>(evt.StateId).ConcurrentTell(bytes);
        }
    }
}
