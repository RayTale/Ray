using Ray.Core.EventSourcing;
using Ray.IGrains.Events;
using Ray.IGrains.States;

namespace Ray.Grain.EventHandles
{
    public class AccountEventHandle : IEventHandle
    {
        public void Apply(object state, IEvent evt)
        {
            if (state is AccountState actorState)
            {
                switch (evt)
                {
                    case AmountAddEvent value: AmountAddEventHandle(actorState, value); break;
                    case AmountTransferEvent value: AmountTransferEventHandle(actorState, value); break;
                    default: break;
                }
            }
        }
        private void AmountTransferEventHandle(AccountState state, AmountTransferEvent evt)
        {
            state.Balance = evt.Balance;
        }
        private void AmountAddEventHandle(AccountState state, AmountAddEvent evt)
        {
            state.Balance = evt.Balance;
        }
    }
}
