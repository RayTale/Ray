using Orleans.Concurrency;
using Ray.Core.Event;
using Ray.DistributedTx;
using Ray.EventBus.Kafka;
using System.Threading.Tasks;
using Transfer.Grains.Events;
using Transfer.IGrains;

namespace Transfer.Grains.Grains
{
    /// <summary>
    /// 该Grain演示了如何在开启并发后保证状态的一致性
    /// 开启并发能提高IO效率,加快处理速度,单Grain的并发要求高的情况下才需要
    /// 支持分布式事务必须添加[Reentrant]
    /// </summary>
    [Reentrant, Producer]
    public sealed class Account : DTxGrain<long, AccountState>, IAccount
    {
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(Snapshot.State.Balance);
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
        public async Task<bool> TransferDeduct(decimal amount, long transactionId)
        {
            bool result = false;
            await ConcurrentTxRaiseEvent(transactionId, async (snapshot, func) =>
            {
                if (snapshot.State.Balance > amount)
                {
                    await func(new TransferDeductEvent { Amount = amount, Balance = snapshot.State.Balance - amount }, null);
                    result = true;
                }
            });
            return result;
        }
        public Task TransferArrived(decimal amount, long transactionId)
        {
            return ConcurrentTxRaiseEvent(transactionId, async (snapshot, func) =>
            {
                var evt = new TransferArrivedEvent
                {
                    Amount = amount,
                    Balance = Snapshot.State.Balance + amount
                };
                await func(evt, null);
            });
        }
    }
}
