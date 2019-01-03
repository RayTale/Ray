using Ray.Core.Abstractions;
using Ray.IGrains.Events;
using Ray.IGrains.States;

namespace Ray.Grain.EventHandles
{
    public class AccountEventHandle : IEventHandler<AccountState>
    {
        public void Apply(AccountState state, IEvent evt)
        {
            switch (evt)
            {
                case AmountAddEvent value: AmountAddEventHandle(state, value); break;
                case AmountTransferEvent value: AmountTransferEventHandle(state, value); break;
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
