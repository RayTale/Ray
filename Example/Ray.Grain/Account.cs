using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.MongoES;
using Ray.Grain.EventHandles;
using Orleans.Concurrency;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;

namespace Ray.Grain
{
    [RabbitMQ.RabbitPub("Account", "account")]
    [MongoStorage("Test", "Account")]
    public sealed class Account : MongoESGrain<String, AccountState, IGrains.MessageInfo>, IAccount
    {
        protected override string GrainId => this.GetPrimaryKeyString();

        static IEventHandle _eventHandle = new AccountEventHandle();
        protected override IEventHandle EventHandle => _eventHandle;

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
        }
        public Task Transfer(string toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, this.State.Balance - amount);
            return RaiseEvent(evt).AsTask();
        }
        public Task AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount, this.State.Balance + amount);
            return RaiseEvent(evt, uniqueId: uniqueId).AsTask();
        }
        [AlwaysInterleave]
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(this.State.Balance);
        }
    }
}
