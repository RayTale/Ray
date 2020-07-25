using System.Threading.Tasks;
using ConcurrentTransfer.Grains.Events;
using ConcurrentTransfer.Grains.States;
using ConcurrentTransfer.IGrains;
using Orleans.Concurrency;
using Ray.Core;
using Ray.Core.Event;
using Ray.EventBus.RabbitMQ;

namespace ConcurrentTransfer.Grains.Grains
{
    /// <summary>
    /// This Grain demonstrates how to ensure state consistency after enabling concurrency
    /// Turning on concurrency can improve IO efficiency and speed up processing, only needed when the concurrency requirements of a single Grain are high
    /// </summary>
    [Reentrant, Producer]
    public sealed class Account : ConcurrentTxGrain<long, AccountState>, IAccount
    {
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(Snapshot.State.Balance);
        }
        public async Task<bool> Transfer(long toAccountId, decimal amount)
        {
            var result = false;
            await ConcurrentRaiseEvent(async (snapshot, func) =>
           {
               if (snapshot.State.Balance > amount)
               {
                   var evt = new TransferEvent
                   {
                       Amount = amount,
                       Balance = Snapshot.State.Balance - amount,
                       ToId = toAccountId
                   };
                   await func(evt, null);
                   result = true;
               }
           });
            return result;
        }
        public Task<bool> TopUp(decimal amount)
        {
            return ConcurrentRaiseEvent(async (snapshot, func) =>
            {
                var evt = new TopupEvent
                {
                    Amount = amount,
                    Balance = Snapshot.State.Balance + amount
                };
                await func(evt, null);
            });
        }

        public Task TransferArrived(decimal amount, EventUID uid)
        {
            return ConcurrentRaiseEvent(async (snapshot, func) =>
            {
                var evt = new TransferArrivedEvent
                {
                    Amount = amount,
                    Balance = Snapshot.State.Balance + amount
                };
                await func(evt, uid);
            });
        }

        public Task<bool> TransferRefunds(decimal amount, EventUID uid)
        {
            return ConcurrentRaiseEvent(async (snapshot, func) =>
            {
                var evt = new TransferRefundsEvent
                {
                    Amount = amount,
                    Balance = Snapshot.State.Balance + amount
                };
                await func(evt, null);
            });
        }
    }
}
