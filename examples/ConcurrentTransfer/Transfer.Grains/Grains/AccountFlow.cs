using Ray.Core;
using Ray.Core.Abstractions.Observer;
using Ray.Core.Event;
using System.Threading.Tasks;
using Transfer.Grains.Events;
using Transfer.IGrains;

namespace Transfer.Grains.Grains
{
    [EventIgnore(typeof(TopupEvent), typeof(TransferArrivedEvent), typeof(TransferRefundsEvent))]
    [Observer(DefaultGroup.primary, DefaultName.Flow, typeof(Account))]
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
