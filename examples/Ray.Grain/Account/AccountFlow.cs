using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.IGrains.Actors;
using Ray.Grain.Events;

namespace Ray.Grain
{
    [Observer(DefaultObserverGroup.primary, "flow", typeof(Account))]
    public sealed class AccountFlow : ConcurrentObserverGrain<Account, long>, IAccountFlow
    {
        readonly IGrainFactory grainFactory;
        public AccountFlow(
            ILogger<AccountFlow> logger,
            IGrainFactory grainFactory) : base(logger)
        {
            this.grainFactory = grainFactory;
        }
        public override long GrainId => this.GetPrimaryKeyLong();
        protected override bool EventConcurrentProcessing => true;
        protected override async ValueTask OnEventDelivered(IFullyEvent<long> fully)
        {
            switch (fully.Event)
            {
                case AmountTransferEvent value: await AmountAddEventHandler(value, new EventUID(fully.GetEventId(), fully.Base.Timestamp)); break;
            }
        }
        public Task AmountAddEventHandler(AmountTransferEvent value, EventUID uid)
        {
            var toActor = grainFactory.GetGrain<IAccount>(value.ToAccountId);
            return toActor.AddAmount(value.Amount, uid);
        }
    }
}
