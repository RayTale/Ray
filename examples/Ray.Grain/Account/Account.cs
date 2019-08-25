using Orleans.Concurrency;
using Ray.Core;
using Ray.Core.Event;
using Ray.DistributedTx;
//using Ray.EventBus.Kafka;
using Ray.EventBus.RabbitMQ;
using Ray.Grain.Events;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using System.Threading.Tasks;

namespace Ray.Grain
{
    [Reentrant, Observable, Producer]
    public sealed class Account : DTxGrain<long, AccountState>, IAccount
    {
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, Snapshot.State.Balance - amount);
            return RaiseEvent(evt);
        }
        public async Task<bool> TransferDeduct(decimal amount, long transactionId)
        {
            bool result = false;
            await ConcurrentTxRaiseEvent(transactionId, async (snapshot, func) =>
             {
                 if (snapshot.State.Balance > amount)
                 {
                     await func(new AmountDeductEvent(amount, snapshot.State.Balance - amount), null);
                     result = true;
                 }
             });
            return result;
        }
        public async Task TransferAddAmount(decimal amount, long transactionId)
        {
            await TxRaiseEvent(transactionId, new AmountAddEvent(amount, Snapshot.State.Balance + amount));
        }
        public Task<bool> AddAmount(decimal amount, EventUID uniqueId = null)
        {
            return ConcurrentRaiseEvent((snapshot, func) =>
           {
               var evt = new AmountAddEvent(amount, snapshot.State.Balance + amount);
               return func(evt, uniqueId);
           });
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(Snapshot.State.Balance);
        }
    }
}
