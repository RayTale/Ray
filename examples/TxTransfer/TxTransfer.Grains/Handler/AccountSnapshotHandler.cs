using Ray.DistributedTx;
using TxTransfer.Grains.Events;
using TxTransfer.Grains.States;

namespace TxTransfer.Grains.Handler
{
    public class AccountSnapshotHandler : DTxSnapshotHandler<long, AccountState>
    {
        public void EventHandle(AccountState state, TopupEvent evt)
        {
            state.Balance = evt.Balance;
        }
        public void EventHandle(AccountState state, TransferArrivedEvent evt)
        {
            state.Balance = evt.Balance;
        }
        public void EventHandle(AccountState state, TransferDeductEvent evt)
        {
            state.Balance = evt.Balance;
        }
    }
}
