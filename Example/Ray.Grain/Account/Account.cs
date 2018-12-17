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
    public sealed class Account : ConcurrentGrain<long, AccountState, IGrains.MessageInfo>, IAccount
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
        public async Task<bool> AddAmount(decimal amount, string uniqueId = null)
        {
            var taskSource = new TaskCompletionSource<bool>();
            var task = ConcurrentRaiseEvent(async (state, eventFunc) =>
            {
                var evt = new AmountAddEvent(amount, State.Balance + amount);
                await eventFunc(evt, uniqueId, null);
            }, result =>
            {
                taskSource.TrySetResult(result);
                return new ValueTask(Task.CompletedTask);
            }, ex =>
            {
                taskSource.TrySetException(ex);
            });
            if (!task.IsCompleted)
                await task;
            return await taskSource.Task;
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
