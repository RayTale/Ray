using Ray.Core.Event;
using Ray.Core.Snapshot;
using Ray.DistributedTransaction;
using Ray.Grain.Events;
using Ray.IGrains.States;

namespace Ray.Grain.EventHandles
{
    public class AccountEventHandle : TransactionEventHandler<long, AccountState>
    {
        public override void CustomApply(Snapshot<long, AccountState> snapshot, IFullyEvent<long> fullyEvent)
        {
            switch (fullyEvent.Event)
            {
                case AmountAddEvent value: AmountAddEventHandle(snapshot.State, value); break;
                case AmountTransferEvent value: AmountTransferEventHandle(snapshot.State, value); break;
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
