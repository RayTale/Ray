using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.IGrains.Actors;
using Ray.Grain.Events;
using Ray.Core.Core.Observer;

namespace Ray.Grain
{
    [Observer(DefaultObserverGroup.primary, typeof(Account), typeof(IAccountFlow))]
    public sealed class AccountFlow : ConcurrentObserverGrain<Account, long>, IAccountFlow
    {
        public AccountFlow(ILogger<AccountFlow> logger) : base(logger)
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();
        protected override bool EventConcurrentProcessing => true;
        protected override async ValueTask OnEventDelivered(IFullyEvent<long> fully)
        {
            switch (fully.Event)
            {
                case AmountTransferEvent value: await AmountAddEventHandler(value); break;
            }
        }
        public Task AmountAddEventHandler(AmountTransferEvent value)
        {
            var toActor = GrainFactory.GetGrain<IAccount>(value.ToAccountId);
            return toActor.AddAmount(value.Amount);
        }
    }
}
