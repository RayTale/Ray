﻿using System.Threading.Tasks;
using Orleans;
using Ray.Core;
using Ray.Core.Event;
using RayTest.IGrains.Actors;
using RayTest.IGrains.Events;
using RayTest.IGrains.States;

namespace RayTest.Grains
{
    public sealed class Account :
        TxGrain<long, AccountState>, IAccount
    {
        public Account() : base()
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
        }
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, Snapshot.State.Balance - amount);
            return RaiseEvent(evt);
        }
        public Task<bool> AddAmount(decimal amount, EventUID uniqueId = null)
        {
            var evt = new AmountAddEvent(amount, Snapshot.State.Balance + amount);
            return RaiseEvent(evt, uniqueId);
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(Snapshot.State.Balance);
        }
    }
}
