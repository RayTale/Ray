using Ray.Core;
using Ray.Core.Event;
using Ray.Core.Observer;
using System.Threading.Tasks;
using Ray.Core.Abstractions.Observer;
using Transfer.Grains.Events;
using Transfer.IGrains;

namespace Transfer.Grains.Grains
{
    [Observer(DefaultGroup.Second, DefaultName.Db, typeof(Account))]
    public sealed class AccountDb : ObserverGrain<long, Account>, IAccountDb
    {
        public Task EventHandle(TransferEvent evt, EventBasicInfo eventBase)
        {
            //Update database here
            return Task.CompletedTask;
        }
        public Task EventHandle(TopupEvent evt, EventBasicInfo eventBase)
        {
            //Update database here
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferArrivedEvent evt, EventBasicInfo eventBase)
        {
            //Update database here
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferRefundsEvent evt, EventBasicInfo eventBase)
        {
            //Update database here
            return Task.CompletedTask;
        }
    }
}
