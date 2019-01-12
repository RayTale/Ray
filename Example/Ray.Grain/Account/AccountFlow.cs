using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;

namespace Ray.Grain
{
    public sealed class AccountFlow : ConcurrentFollowGrain<long, EventBase<long>, AsyncState<long>, MessageInfo>, IAccountFlow
    {
        public AccountFlow(ILogger<AccountFlow> logger) : base(logger)
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();
        protected override bool EventConcurrentProcessing => true;
        protected override async ValueTask OnEventDelivered(IEvent<long, EventBase<long>> @event)
        {
            switch (@event)
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
