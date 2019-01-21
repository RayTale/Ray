using Ray.Core.Event;
using RayTest.IGrains.Events;
using RayTest.IGrains.States;

namespace RayTest.Grains.EventHandles
{
    public class AccountEventHandle : IEventHandler<long, EventBase<long>, AccountState, StateBase<long>>
    {
        public void Apply(AccountState state, IEvent<long, EventBase<long>> evt)
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
