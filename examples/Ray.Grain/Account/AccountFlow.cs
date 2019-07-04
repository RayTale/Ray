using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.Grain.Events;
using Ray.IGrains.Actors;
using System.Threading.Tasks;

namespace Ray.Grain
{
    [Observer(DefaultObserverGroup.primary, "flow", typeof(Account))]
    public sealed class AccountFlow : ConcurrentObserverGrain<Account, long>, IAccountFlow
    {
        readonly IGrainFactory grainFactory;
        public AccountFlow(IGrainFactory grainFactory)
        {
            this.grainFactory = grainFactory;
        }
        protected override bool ConcurrentHandle => true;
        public Task EventHandler(AmountTransferEvent value, EventBase eventBase)
        {
            var toActor = grainFactory.GetGrain<IAccount>(value.ToAccountId);
            return toActor.AddAmount(value.Amount, new EventUID(eventBase.GetEventId(GrainId.ToString()), eventBase.Timestamp));
        }
        public Task EventHandler(AmountAddEvent evt)
        {
            return Task.CompletedTask;
        }
    }
}
