using Ray.Core.Event;
using Ray.Core.Snapshot;
using Ray.IGrains.Events;
using Ray.IGrains.States;

namespace Ray.Grain.EventHandles
{
    public class AccountEventHandle : IEventHandler<long, AccountState>
    {
        public void Apply(Snapshot<long, AccountState> state, IFullyEvent<long> fully)
        {
            switch (fully.Event)
            {
                case AmountAddEvent value: AmountAddEventHandle(state.State, value); break;
                case AmountTransferEvent value: AmountTransferEventHandle(state.State, value); break;
                default: break;
            }
        }
        private void AmountTransferEventHandle(AccountState state, AmountTransferEvent evt)
        {
            state.Balance = evt.Balance;
        }
        private void AmountAddEventHandle(AccountState state, AmountAddEvent evt)
        {
            state.Balance += evt.Amount;
        }
    }
}
