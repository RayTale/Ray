using Ray.Core;
using Ray.Core.Event;
using Ray.Core.Observer;
using System.Threading.Tasks;
using RayTest.Grains.Events;
using RayTest.IGrains;

namespace RayTest.Grains.Grains
{
    [EventIgnore(typeof(TopupEvent), typeof(TransferArrivedEvent), typeof(TransferRefundsEvent))]
    [Observer(DefaultObserverGroup.primary, "flow", typeof(Account))]
    public sealed class AccountFlow : ObserverGrain<long, Account>, IAccountFlow
    {
        public Task EventHandle(TransferEvent evt, EventUID uid)
        {
            var toActor = GrainFactory.GetGrain<IAccount>(evt.ToId);
            return toActor.TransferArrived(evt.Amount, uid);
        }
    }
}
