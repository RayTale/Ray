using Ray.Core;
using Ray.Core.Event;
using Ray.Core.Observer;
using System.Threading.Tasks;
using RayTest.Grains.Events;
using RayTest.IGrains;

namespace RayTest.Grains.Grains
{
    [Observer(DefaultObserverGroup.secondary, "db", typeof(Account))]
    public sealed class AccountDb : ObserverGrain<long, Account>, IAccountFlow
    {
        public Task EventHandle(TransferEvent evt, EventBase eventBase)
        {
            //此处更新db
            return Task.CompletedTask;
        }
        public Task EventHandle(TopupEvent evt, EventBase eventBase)
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
