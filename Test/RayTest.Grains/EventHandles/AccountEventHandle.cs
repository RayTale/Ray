using Ray.Core.Abstractions;
using RayTest.IGrains.Events;
using RayTest.IGrains.States;

namespace RayTest.Grains.EventHandles
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
