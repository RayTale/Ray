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
    public sealed class AccountCoreHandler : PartSubHandler<string, MessageInfo>
    {
        public AccountCoreHandler()
        {
            Register<AmountTransferEvent>();
        }
        public override Task Tell(byte[] bytes, IActorOwnMessage<string> data, MessageInfo msg)
        {
            switch (data)
            {
                case AmountTransferEvent value: return AmountAddEventHandler(value);
                default: return Task.CompletedTask;
            }
        }
        public Task AmountAddEventHandler(AmountTransferEvent value)
        {
            var toActor = HandlerStart.Client.GetGrain<IAccount>(value.ToAccountId);
            return toActor.AddAmount(value.Amount, value.Id);
        }
    }
}
