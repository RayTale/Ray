using Ray.Core;
using Ray.Core.Abstractions.Observer;
using Ray.Core.Event;
using RayTest.Grains.Events;
using RayTest.IGrains;
using System.Threading.Tasks;

namespace RayTest.Grains.Grains
{
    [Observer(DefaultName.Db, typeof(Account))]
    public sealed class AccountDb : ObserverGrain<long, Account>, IAccountFlow
    {
        public Task EventHandle(TransferEvent evt, EventBasicInfo eventBase)
        {
            //此处更新db
            return Task.CompletedTask;
        }
        public Task EventHandle(TopupEvent evt, EventBasicInfo eventBase)
        {
            //此处更新db
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferArrivedEvent evt, EventBasicInfo eventBase)
        {
            //此处更新db
            return Task.CompletedTask;
        }
        public Task EventHandle(TransferRefundsEvent evt, EventBasicInfo eventBase)
        {
            //此处更新db
            return Task.CompletedTask;
        }
    }
}
