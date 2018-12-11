using Ray.Core.Messaging;
using Ray.RabbitMQ;
using System.Threading.Tasks;
using Ray.IGrains;
using System;
using Ray.Core.EventBus;
using Ray.Core;
using Ray.IGrains.Actors;
using Ray.Core.Internal;

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
                return clientFactory.GetClient().GetGrain<IAccountDb>(evt.StateId).ConcurrentTell(wrapBytes);
            return Task.CompletedTask;
        }
    }
}
