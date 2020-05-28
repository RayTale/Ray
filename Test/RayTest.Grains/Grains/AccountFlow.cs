using Ray.Core;
using Ray.Core.Abstractions.Observer;
using Ray.Core.Event;
using RayTest.Grains.Events;
using RayTest.IGrains;
using System.Threading.Tasks;

namespace RayTest.Grains.Grains
{
    [EventIgnore(typeof(TopupEvent), typeof(TransferArrivedEvent), typeof(TransferRefundsEvent))]
    [Observer(DefaultName.Flow, typeof(Account))]
    public sealed class AccountFlow : ObserverGrain<long, Account>, IAccountFlow
    {
        public Task EventHandle(TransferEvent evt, EventUID uid)
        {
            var toActor = GrainFactory.GetGrain<IAccount>(evt.ToId);
            return toActor.TransferArrived(evt.Amount, uid);
        }
    }
}
