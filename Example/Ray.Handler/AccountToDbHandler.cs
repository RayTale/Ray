using System;
using System.Threading.Tasks;
using Ray.Core.Client;
using Ray.Core.EventBus;
using Ray.Core.Internal;
using Ray.IGrains;
using Ray.IGrains.Actors;

namespace Ray.Handler
{
    public sealed class AccountToDbHandler : SubHandler<MessageInfo>
    {
        IClientFactory clientFactory;
        public AccountToDbHandler(IServiceProvider svProvider, IClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        public override Task Tell(byte[] wrapBytes, byte[] dataBytes, object data, MessageInfo msg)
        {
            if (data is IEventBase<long> evt)
                return clientFactory.Create().GetGrain<IAccountDb>(evt.StateId).ConcurrentTell(wrapBytes);
            return Task.CompletedTask;
        }
    }
}
