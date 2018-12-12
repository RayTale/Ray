using System.Threading.Tasks;
using Orleans;
using Ray.Core.Internal;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.Grain.EventHandles;
using Microsoft.Extensions.Logging;

namespace Ray.Grain
{
    public sealed class Account : TransactionGrain<long, AccountState, IGrains.MessageInfo>, IAccount
    {
        public Account(ILogger<Account> logger) : base(logger)
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();

        public static IEventHandle<AccountState> EventHandle { get; } = new AccountEventHandle();
        protected override void Apply(AccountState state, IEventBase<long> evt)
        {
            EventHandle.Apply(state, evt);
        }
        protected override bool SupportAsyncFollow => true;
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
        }
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, this.State.Balance - amount);
            return RaiseEvent(evt);
        }
        public Task<bool> AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount, State.Balance + amount);
            return ConcurrentInput(evt, uniqueId);
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
