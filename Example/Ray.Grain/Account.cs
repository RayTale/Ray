using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.MongoES;

namespace Ray.Grain
{
    [RabbitMQ.RabbitPub("Account", "account")]
    [MongoStorage("Test", "Account")]
    public sealed class Account : MongoESGrain<String, AccountState, IGrains.MessageInfo>, IAccount
    {
        protected override string GrainId => this.GetPrimaryKeyString();

        private ObserverSubscriptionManager<IAccountReplicated> _subsManager;
        public override Task OnActivateAsync()
        {
            _subsManager = new ObserverSubscriptionManager<IAccountReplicated>();
            var replicatedRef = GrainFactory.GetGrain<IAccountReplicated>(this.GrainId);
            _subsManager.Subscribe(replicatedRef);
            return base.OnActivateAsync();
        }
        protected override void RaiseEventAfter(IEventBase<string> @event, byte[] bytes)
        {
            var message = new IGrains.MessageInfo() { TypeCode = @event.TypeCode, BinaryBytes = bytes };
            _subsManager.Notify(s => s.Tell(message));
        }
        public Task Transfer(string toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, this.State.Balance - amount);
            return RaiseEvent(evt);
        }
        public Task AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount, this.State.Balance + amount);
            return RaiseEvent(evt, uniqueId: uniqueId);
        }

        public Task<decimal> GetBalance()
        {
            return Task.FromResult(this.State.Balance);
        }
    }
}
