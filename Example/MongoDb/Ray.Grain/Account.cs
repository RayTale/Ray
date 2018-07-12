using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.MongoDB;
using Ray.Grain.EventHandles;
using Orleans.Concurrency;
using Ray.RabbitMQ;
using System.Threading.Tasks.Dataflow;
using System;

namespace Ray.Grain
{
    [RabbitPub("Account", "account")]
    public sealed class Account : MongoGrain<long, AccountState, IGrains.MessageInfo>, IAccount
    {
        protected override long GrainId => this.GetPrimaryKeyLong();

        static IEventHandle _eventHandle = new AccountEventHandle();
        protected override IEventHandle EventHandle => _eventHandle;
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
        BufferBlock<TaskWrap<AmountAddEvent>> addAmountBufferBlock;
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            addAmountBufferBlock = new BufferBlock<TaskWrap<AmountAddEvent>>();
            RegisterTimer(async (state) =>
            {
                if (addAmountBufferBlock.TryReceiveAll(out var events))
                {
                    try
                    {
                        await BeginTransaction();
                        foreach (var evt in events)
                        {
                            await RaiseEvent(evt.Value, evt.UniqueId);
                        }
                        await CommitTransaction();
                        foreach (var evt in events)
                        {
                            evt.TaskSource.SetResult(true);
                        }
                    }
                    catch
                    {
                        await RollbackTransaction();
                        foreach (var evt in events)
                        {
                            try
                            {
                                evt.TaskSource.TrySetResult(await RaiseEvent(evt.Value, evt.UniqueId));
                            }
                            catch (Exception e)
                            {
                                evt.TaskSource.TrySetException(e);
                            }
                        }
                    }
                }
                Console.WriteLine($"Callback:{DateTime.Now.ToString()}");
            }, null, new TimeSpan(0, 0, 0, 0, 100), new TimeSpan(0, 0, 0, 0, 100));
        }
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, State.Balance - amount);
            return RaiseEvent(evt).AsTask();
        }
        public async Task AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount, State.Balance + amount);
            var task = TaskWrap<AmountAddEvent>.Create(evt, uniqueId);
            await addAmountBufferBlock.SendAsync(task);
            await task.TaskSource.Task;
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
