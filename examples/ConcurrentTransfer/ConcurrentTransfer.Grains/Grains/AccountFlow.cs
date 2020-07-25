using System.Threading.Tasks;
using ConcurrentTransfer.Grains.Events;
using ConcurrentTransfer.IGrains;
using Ray.Core;
using Ray.Core.Abstractions.Observer;
using Ray.Core.Event;

namespace ConcurrentTransfer.Grains.Grains
{
    [EventIgnore(typeof(TopupEvent), typeof(TransferArrivedEvent), typeof(TransferRefundsEvent))]
    [Observer(DefaultGroup.Primary, DefaultName.Flow, typeof(Account))]
    public sealed class AccountFlow : ObserverGrain<long, Account>, IAccountFlow
    {
        protected override bool ConcurrentHandle => true;
        public async Task EventHandle(TransferEvent evt, EventUID uid)
        {
            var toActor = GrainFactory.GetGrain<IAccount>(evt.ToId);
            await toActor.TransferArrived(evt.Amount, uid);
        }
    }
}
