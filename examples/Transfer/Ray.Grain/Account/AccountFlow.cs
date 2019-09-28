using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.DistributedTx;
using Ray.Grain.Events;
using Ray.IGrains.Actors;
using System.Threading.Tasks;

namespace Ray.Grain
{
    [Observer(DefaultObserverGroup.primary, "flow", typeof(Account))]
    public sealed class AccountFlow : DTxObserverGrain<long, Account>, IAccountFlow
    {
        protected override bool ConcurrentHandle => true;
        public Task EventHandler(AmountTransferEvent value, EventBase eventBase)
        {
            var toActor = GrainFactory.GetGrain<IAccount>(value.ToAccountId);
            return toActor.AddAmount(value.Amount, new EventUID(eventBase.GetEventId(GrainId.ToString()), eventBase.Timestamp));
        }
        public Task EventHandler(AmountAddEvent evt)
        {
            return Task.CompletedTask;
        }
        public Task EventHandle(AmountDeductEvent evt)
        {
            return Task.CompletedTask;
        }
    }
}
