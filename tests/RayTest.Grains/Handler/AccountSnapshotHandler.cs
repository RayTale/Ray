﻿using Ray.Core.Snapshot;
using RayTest.Grains.Events;

namespace RayTest.Grains.Handler
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
