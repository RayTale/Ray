using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.RabbitMQ;
using Microsoft.Extensions.Options;
using Ray.PostgreSQL;
using RayTest.IGrains.States;
using RayTest.IGrains.Actors;
using RayTest.Grains.EventHandles;
using RayTest.IGrains.Events;

namespace RayTest.Grains
{
    public sealed class Account : TransactionGrain<long, AccountState, IGrains.MessageInfo>, IAccount
    {
        public override long GrainId => this.GetPrimaryKeyLong();

        public static IEventHandle<AccountState> EventHandle { get; } = new AccountEventHandle();
        protected override void Apply(AccountState state, IEventBase<long> evt)
        {
            EventHandle.Apply(state, evt);
        }
        protected override bool SupportAsync => true;
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
