using System.Threading.Tasks;
using ConcurrentTransfer.Grains.Events;
using ConcurrentTransfer.IGrains;
using Ray.Core;
using Ray.Core.Abstractions.Observer;
using Ray.Core.Event;

namespace ConcurrentTransfer.Grains.Grains
{
    [Observer(DefaultGroup.Second, DefaultName.Db, typeof(Account))]
    public sealed class AccountDb : ObserverGrain<long, Account>, IAccountDb
    {
        public Task EventHandle(TransferEvent evt, EventBasicInfo eventBase)
        {
            //Update db here
            return Task.CompletedTask;
        }
        public Task EventHandle(TopupEvent evt, EventBasicInfo eventBase, EventUID fullyEvent)
        {
            //Update db here
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferArrivedEvent evt, EventBasicInfo eventBase)
        {
            //Update db here
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferRefundsEvent evt, EventBasicInfo eventBase)
        {
            //Update db here
            return Task.CompletedTask;
        }
    }
}
