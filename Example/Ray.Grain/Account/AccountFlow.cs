using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;

namespace Ray.Grain
{
    public sealed class AccountFlow : AsyncGrain<long, AsyncState<long>, MessageInfo>, IAccountFlow
    {
        public override long GrainId => this.GetPrimaryKeyLong();
        protected override bool Concurrent => true;
        protected override async ValueTask OnEventDelivered(IEventBase<long> @event)
        {
            switch (@event)
            {
                case AmountTransferEvent value: await AmountAddEventHandler(value); break;
            }
        }
        public Task AmountAddEventHandler(AmountTransferEvent value)
        {
            var toActor = GrainFactory.GetGrain<IAccount>(value.ToAccountId);
            return toActor.AddAmount(value.Amount, value.GetUniqueId());
        }
    }
}
