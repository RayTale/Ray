using Ray.Core;
using Ray.Core.Event;
using Ray.EventBus.Kafka;
using System.Threading.Tasks;
using Transfer.Grains.Events;
using Transfer.IGrains;

namespace Transfer.Grains.Grains
{
    [Producer]
    public sealed class Account : RayGrain<long, AccountState>, IAccount
    {
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(Snapshot.State.Balance);
        }
        public Task<bool> Transfer(long toAccountId, decimal amount)
        {
            if (Snapshot.State.Balance >= amount)
            {
                var evt = new TransferEvent
                {
                    Amount = amount,
                    Balance = Snapshot.State.Balance - amount,
                    ToId = toAccountId
                };
                return RaiseEvent(evt);
            }
            else
                return Task.FromResult(false);
        }
        public Task<bool> TopUp(decimal amount)
        {
            var evt = new TopupEvent
            {
                Amount = amount,
                Balance = Snapshot.State.Balance + amount
            };
            return RaiseEvent(evt);
        }

        public Task TransferArrived(decimal amount, EventUID uid)
        {
            var evt = new TransferArrivedEvent
            {
                Amount = amount,
                Balance = Snapshot.State.Balance + amount
            };
            return RaiseEvent(evt, uid);
        }

        public Task<bool> TransferRefunds(decimal amount, EventUID uid)
        {
            var evt = new TransferRefundsEvent
            {
                Amount = amount,
                Balance = Snapshot.State.Balance + amount
            };
            return RaiseEvent(evt, uid);
        }
    }
}
