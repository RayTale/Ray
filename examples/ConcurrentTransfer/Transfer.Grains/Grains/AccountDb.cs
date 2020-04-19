using Ray.Core;
using Ray.Core.Event;
using Ray.Core.Observer;
using System.Threading.Tasks;
using Transfer.Grains.Events;
using Transfer.IGrains;

namespace Transfer.Grains.Grains
{
    [Observer(DefaultObserverGroup.secondary, "db", typeof(Account))]
    public sealed class AccountDb : ObserverGrain<long, Account>, IAccountDb
    {
        public Task EventHandle(TransferEvent evt, EventBase eventBase)
        {
            //此处更新db
            return Task.CompletedTask;
        }
        public Task EventHandle(TopupEvent evt, EventBase eventBase, EventUID fullyEvent)
        {
            //此处更新db
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferArrivedEvent evt, EventBase eventBase)
        {
            //此处更新db
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferRefundsEvent evt, EventBase eventBase)
        {
            //此处更新db
            return Task.CompletedTask;
        }
    }
}
