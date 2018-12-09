using Ray.Core;
using Ray.Core.EventSourcing;
using Ray.Core.MQ;
using Ray.IGrains;
using Ray.IGrains.Actors;
using System;
using System.Threading.Tasks;

namespace Ray.Handler
{
    public sealed class AccountRepHandler : MultHandler<long, MessageInfo>
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
