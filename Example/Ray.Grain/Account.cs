using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.MongoES;
using Ray.Grain.EventHandles;

namespace Ray.Grain
{
    [RabbitMQ.RabbitPub("Account", "account")]
    [MongoStorage("Test", "Account")]
    public sealed class Account : MongoESGrain<String, AccountState, IGrains.MessageInfo>, IAccount
    {
        protected override string GrainId => this.GetPrimaryKeyString();

        static IEventHandle _eventHandle = new AccountEventHandle();
        protected override IEventHandle EventHandle => _eventHandle;

        private ObserverSubscriptionManager<IAccountReplicated> _subsManager;
        public override Task OnActivateAsync()
        {
            _subsManager = new ObserverSubscriptionManager<IAccountReplicated>();
            var replicatedRef = GrainFactory.GetGrain<IAccountReplicated>(this.GrainId);
            _subsManager.Subscribe(replicatedRef);
            return base.OnActivateAsync();
        }
        protected override async Task AfterEventSavedHandle(IEventBase<string> @event, byte[] bytes, int recursion = 0, string mqHashKey = null)
        {
            await base.AfterEventSavedHandle(@event, bytes, recursion, mqHashKey);
            var message = new IGrains.MessageInfo() { TypeCode = @event.TypeCode, BinaryBytes = bytes };
            _subsManager.Notify(s => s.Tell(message));
        }
        public Task Transfer(string toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, this.State.Balance - amount);
            return RaiseEvent(evt).AsTask();
        }
        public Task AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount, this.State.Balance + amount);
            return RaiseEvent(evt, uniqueId: uniqueId).AsTask();
        }

        public Task<decimal> GetBalance()
        {
            return Task.FromResult(this.State.Balance);
        }
    }
}
