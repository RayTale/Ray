using Ray.Core.Message;
using Ray.RabbitMQ;
using System.Threading.Tasks;
using Ray.IGrains;
using System;
using Ray.Core.MQ;
using Ray.Core;
using Ray.IGrains.Actors;
using Ray.Core.EventSourcing;

namespace Ray.Handler
{
    [RabbitSub("Read", "Account", "account", QueueCount = 20)]
    public sealed class AccountToDbHandler : SubHandler<MessageInfo>
    {
        IClientFactory clientFactory;
        public AccountToDbHandler(IServiceProvider svProvider, IClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }

        public override Task Tell(byte[] wrapBytes, byte[] dataBytes, IMessage data, MessageInfo msg)
        {
            if (data is IEventBase<long> evt)
                return clientFactory.GetClient().GetGrain<IAccountDb>(evt.StateId).ConcurrentTell(wrapBytes);
            return Task.CompletedTask;
        }
    }
}
