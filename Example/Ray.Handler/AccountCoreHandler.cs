using Ray.Core.MQ;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.RabbitMQ;
using System.Threading.Tasks;

namespace Ray.Handler
{
    [RabbitSub("Core", "Account", "account")]
    public class AccountCoreHandler : PartSubHandler<MessageInfo>
    {
        public AccountCoreHandler()
        {
            Register<AmountTransferEvent>(AmountAddEventHandler);
        }
        public Task AmountAddEventHandler(MessageInfo msg, object evt)
        {
            if (evt is AmountTransferEvent value)
            {
                var toActor = HandlerStart.Client.GetGrain<IAccount>(value.ToAccountId);
                return toActor.AddAmount(value.Amount, value.Id);
            }
            return Task.CompletedTask;
        }
    }
}
