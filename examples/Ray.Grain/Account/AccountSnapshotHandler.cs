﻿using Ray.DistributedTransaction;
using Ray.Grain.Events;
using Ray.IGrains.States;

namespace Ray.Grain.EventHandles
{
    public class AccountSnapshotHandler : TxSnapshotHandler<long, AccountState>
    {
        public void EventHandle(AccountState state, AmountTransferEvent evt)
        {
            state.Balance = evt.Balance;
        }
        public void EventHandle(AccountState state, AmountAddEvent evt)
        {
            state.Balance += evt.Amount;
        }
    }
}
