using ConcurrentTransfer.Grains.Events;
using ConcurrentTransfer.Grains.States;
using Ray.Core.Snapshot;

namespace ConcurrentTransfer.Grains.Handler
{
    public class AccountSnapshotHandler : SnapshotHandler<long, AccountState>
    {
        public void EventHandle(AccountState state, TopupEvent evt)
        {
            state.Balance = evt.Balance;
        }
        public void EventHandle(AccountState state, TransferArrivedEvent evt)
        {
            state.Balance = evt.Balance;
        }
        public void EventHandle(AccountState state, TransferEvent evt)
        {
            state.Balance = evt.Balance;
        }
        public void EventHandle(AccountState state, TransferRefundsEvent evt)
        {
            state.Balance = evt.Balance;
        }
    }
}
