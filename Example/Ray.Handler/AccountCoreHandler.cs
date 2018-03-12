using Ray.Core;
using Ray.Core.Message;
using Ray.Core.MQ;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.RabbitMQ;
using System;
using System.Threading.Tasks;

namespace Ray.Handler
{
    [RabbitSub("Core", "Account", "account")]
    public sealed class AccountCoreHandler : SubHandler<string, MessageInfo>
    {
        IOrleansClientFactory clientFactory;
        public AccountCoreHandler(IServiceProvider svProvider, IOrleansClientFactory clientFactory) : base(svProvider)
        {
            this.clientFactory = clientFactory;
        }
        public override Task Tell(byte[] bytes, IActorOwnMessage<string> data, MessageInfo msg)
        {
            var replicatedRef = clientFactory.GetClient().GetGrain<IAccountRep>(data.StateId);
            var task = replicatedRef.Tell(bytes);
            switch (data)
            {
                case AmountTransferEvent value: return Task.WhenAll(task, AmountAddEventHandler(value));
                default: return task;
            }
        }
        public Task AmountAddEventHandler(AmountTransferEvent value)
        {
            var toActor = clientFactory.GetClient().GetGrain<IAccount>(value.ToAccountId);
            return toActor.AddAmount(value.Amount, value.Id);
        }
    }
}
