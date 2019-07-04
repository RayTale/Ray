using Ray.DistributedTransaction;
using Ray.Grain.Events;
using Ray.IGrains.States;

namespace Ray.Grain.EventHandles
{
    public class AccountSnapshotHandler : TxSnapshotHandler<long, AccountState>
    {
        public static void EventHandle(AccountState state, AmountTransferEvent evt)
        {
            state.Balance = evt.Balance;
        }
        public static void EventHandle(AccountState state, AmountAddEvent evt)
        {
            state.Balance += evt.Amount;
        }
    }
}
