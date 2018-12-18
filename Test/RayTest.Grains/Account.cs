using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Internal;
using RayTest.IGrains.Actors;
using RayTest.IGrains.Events;
using RayTest.IGrains.States;

namespace RayTest.Grains
{
    public sealed class Account : TransactionGrain<long, AccountState, IGrains.MessageInfo>, IAccount
    {
        public Account(ILogger<Account> logger) : base(logger)
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();
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
            return RaiseEvent(evt, uniqueId);
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
