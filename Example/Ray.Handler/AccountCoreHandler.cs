using Ray.Core.Message;
using Ray.Core.MQ;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.RabbitMQ;
using System.Threading.Tasks;

namespace Ray.Handler
{
    [RabbitSub("Core", "Account", "account")]
    public sealed class AccountCoreHandler : SubHandler<string, MessageInfo>
    {
        public override Task Tell(byte[] bytes, IActorOwnMessage<string> data, MessageInfo msg)
        {
            var replicatedRef = HandlerStart.Client.GetGrain<IAccountRep>(data.StateId);
            var task = replicatedRef.Tell(bytes);
            switch (data)
            {
                case AmountTransferEvent value: return Task.WhenAll(task, AmountAddEventHandler(value));
                default: return task;
            }
        }
        public Task AmountAddEventHandler(AmountTransferEvent value)
        {
            var toActor = HandlerStart.Client.GetGrain<IAccount>(value.ToAccountId);
            return toActor.AddAmount(value.Amount, value.Id);
        }
    }
}
