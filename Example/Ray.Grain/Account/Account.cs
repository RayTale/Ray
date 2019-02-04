using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;

namespace Ray.Grain
{
    public sealed class Account : ConcurrentGrain<Account, long, AccountState>, IAccount
    {
        public Account(ILogger<Account> logger) : base(logger)
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, Snapshot.State.Balance - amount);
            return RaiseEvent(evt);
        }
        public async Task<bool> AddAmount(decimal amount, EventUID uniqueId = null)
        {
            var taskSource = new TaskCompletionSource<bool>();
            var task = ConcurrentRaiseEvent((snapshot, eventFunc) =>
           {
               var evt = new AmountAddEvent(amount, Snapshot.State.Balance + amount);
               return eventFunc(evt, uniqueId);
           }, isOk =>
           {
               taskSource.TrySetResult(isOk);
               return Consts.ValueTaskDone;
           }, ex =>
           {
               taskSource.TrySetException(ex);
           });
            if (!task.IsCompletedSuccessfully)
                await task;
            return await taskSource.Task;
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(Snapshot.State.Balance);
        }
    }
}
