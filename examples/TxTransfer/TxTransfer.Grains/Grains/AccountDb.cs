using System.Threading.Tasks;
using Ray.Core.Abstractions.Observer;
using Ray.Core.Event;
using Ray.DistributedTx;
using TxTransfer.Grains.Events;
using TxTransfer.IGrains;

namespace TxTransfer.Grains.Grains
{
    [Observer(DefaultGroup.Second, DefaultName.Db, typeof(Account))]
    public sealed class AccountDb : DTxObserverGrain<long, Account>, IAccountDb
    {
        public Task EventHandle(TransferDeductEvent evt, EventBasicInfo eventBase)
        {
            //Update db here
            return Task.CompletedTask;
        }
        public Task EventHandle(TopupEvent evt, EventBasicInfo eventBase)
        {
            //Update db here
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferArrivedEvent evt, EventBasicInfo eventBase)
        {
            //Update db here
            return Task.CompletedTask;
        }
    }
}
