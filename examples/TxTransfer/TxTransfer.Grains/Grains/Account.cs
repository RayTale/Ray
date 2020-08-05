using System.Threading.Tasks;
using Orleans.Concurrency;
using Ray.Core.Event;
using Ray.DistributedTx;
using Ray.EventBus.RabbitMQ;
using TxTransfer.Grains.Events;
using TxTransfer.Grains.States;
using TxTransfer.IGrains;

namespace TxTransfer.Grains.Grains
{
    /// <summary>
    /// This Grain demonstrates how to ensure state consistency after enabling concurrency
    /// Turning on concurrency can improve IO efficiency and speed up processing, only needed when the concurrency requirements of a single Grain are high
    /// To support distributed transactions must be add <see cref="ReentrantAttribute"/>.
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
                await func(evt, uid);
            });
        }
        public async Task<bool> TransferDeduct(decimal amount, string transactionId)
        {
            bool result = false;
            await ConcurrentTxRaiseEvent(transactionId.ToString(), async (snapshot, func) =>
            {
                if (snapshot.State.Balance > amount)
                {
                    await func(new TransferDeductEvent { Amount = amount, Balance = snapshot.State.Balance - amount }, null);
                    result = true;
                }
            });
            return result;
        }
        public Task TransferArrived(decimal amount, string transactionId)
        {
            return ConcurrentTxRaiseEvent(transactionId.ToString(), async (snapshot, func) =>
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
