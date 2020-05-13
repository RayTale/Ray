using Orleans.Concurrency;
using Ray.Core;
using Ray.Core.Event;
using Ray.EventBus.RabbitMQ;
using System.Threading.Tasks;
using Transfer.Grains.Events;
using Transfer.IGrains;

namespace Transfer.Grains.Grains
{
    /// <summary>
    /// 该Grain演示了如何在开启并发后保证状态的一致性
    /// 开启并发能提高IO效率,加快处理速度,单Grain的并发要求高的情况下才需要
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
                await func(evt, null);
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
