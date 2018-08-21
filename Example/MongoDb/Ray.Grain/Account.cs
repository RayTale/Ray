using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.MongoDB;
using Ray.Grain.EventHandles;
using Ray.RabbitMQ;

namespace Ray.Grain
{
    [RabbitPub("Account", "account")]
    public sealed class Account : MongoGrain<long, AccountState, IGrains.MessageInfo>, IAccount
    {
        protected override long GrainId => this.GetPrimaryKeyLong();

        public static IEventHandle<AccountState> EventHandle { get; } = new AccountEventHandle();
        protected override void Apply(AccountState state, IEventBase<long> evt)
        {
            EventHandle.Apply(state, evt);
        }
        static MongoGrainConfig _ESMongoInfo;
        public override MongoGrainConfig GrainConfig
        {
            get
            {
                if (_ESMongoInfo == null)
                    _ESMongoInfo = new MongoGrainConfig("Test", "Account_Event", "Account_State");
                return _ESMongoInfo;
            }
        }
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, State.Balance - amount);
            return RaiseEvent(evt).AsTask();
        }
        public ValueTask<bool> AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount, State.Balance + amount);
            return RaiseEvent(evt, uniqueId);
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
    public class TaskWrap<T>
    {
        public static TaskWrap<CT> Create<CT>(CT value, string uniqueId = null)
        {
            return new TaskWrap<CT>
            {
                TaskSource = new TaskCompletionSource<bool>(),
                Value = value,
                UniqueId = uniqueId
            };
        }
        public TaskCompletionSource<bool> TaskSource { get; set; }
        public T Value { get; set; }
        public string UniqueId { get; set; }
    }
}
