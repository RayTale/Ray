using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Internal;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;

namespace Ray.Grain
{
    public sealed class Account : ConcurrentGrain<long, AccountState, IGrains.MessageInfo>, IAccount
    {
        public Account(ILogger<Account> logger) : base(logger)
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, this.State.Balance - amount);
            return RaiseEvent(evt);
        }
        public async Task<bool> AddAmount(decimal amount, EventUID uniqueId = null)
        {
            var taskSource = new TaskCompletionSource<bool>();
            var task = ConcurrentRaiseEvent(async (state, eventFunc) =>
            {
                var evt = new AmountAddEvent(amount, State.Balance + amount);
                await eventFunc(evt, uniqueId);
            }, isOk =>
            {
                taskSource.TrySetResult(isOk);
                return new ValueTask();
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
