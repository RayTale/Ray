using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.PostgreSQL;
using Ray.Grain.EventHandles;
using Orleans.Concurrency;
using Ray.RabbitMQ;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using System.Threading;
using System.Collections.Generic;

namespace Ray.Grain
{
    [RabbitPub("Account", "account")]
    public sealed class Account : SqlGrain<long, AccountState, IGrains.MessageInfo>, IAccount
    {
        SqlConfig config;
        public Account(IOptions<SqlConfig> configOptions)
        {
            config = configOptions.Value;
        }

        protected override long GrainId => this.GetPrimaryKeyLong();

        static IEventHandle _eventHandle = new AccountEventHandle();
        protected override IEventHandle EventHandle => _eventHandle;
        static SqlGrainConfig _table;
        protected override bool SupportAsync => false;
        public override SqlGrainConfig GrainConfig
        {
            get
            {
                if (_table == null)
                {
                    _table = new SqlGrainConfig(config.ConnectionDict["core_event"], "account_event", "account_state");
                }
                return _table;
            }
        }
        BufferBlock<EventTransactionWrap<long>> addAmountBufferBlock;
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            addAmountBufferBlock = new BufferBlock<EventTransactionWrap<long>>();
            RegisterTimer(Trans, null, new TimeSpan(0, 0, 5), new TimeSpan(0, 0, 5));
        }
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, this.State.Balance - amount);
            return RaiseEvent(evt).AsTask();
        }
        private async Task Trans(object state)
        {
            if (Interlocked.CompareExchange(ref process, 1, 0) == 0)
            {
                Console.WriteLine("enter process");
                while (await BatchProcess()) { }
                Interlocked.Exchange(ref process, 0);
            }
        }
        public async ValueTask<bool> BatchProcess()
        {
            if (addAmountBufferBlock.TryReceiveAll(out var firstBlock))
            {
                await Task.Delay(50);
                int counts = 0;
                var events = new List<EventTransactionWrap<long>>(firstBlock);
                while (addAmountBufferBlock.TryReceiveAll(out var block))
                {
                    await Task.Delay(10);
                    events.AddRange(block);
                    counts++;
                    if (counts > 5) break;
                }
                try
                {
                    await BeginTransaction();
                    foreach (var evt in events)
                    {
                        await RaiseEvent(evt.Value, evt.UniqueId, isTransaction: true);
                    }
                    var commited = await CommitTransaction();
                    if (commited)
                    {
                        foreach (var evt in events)
                        {
                            evt.TaskSource.SetResult(true);
                        }
                    }
                    else
                    {
                        await ReTry(events);
                    }
                }
                catch
                {
                    await RollbackTransaction();
                    await ReTry(events);
                }
                return true;
            }
            return false;
        }
        public async Task ReTry(IList<EventTransactionWrap<long>> events)
        {
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
        int process = 0;
        public async Task AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount);
            //await RaiseEvent(evt, uniqueId: uniqueId);
            var task = EventTransactionWrap<long>.Create(evt, uniqueId);
            await addAmountBufferBlock.SendAsync(task);
            await Trans(null);
            await task.TaskSource.Task;
            //var stopWatch = new Stopwatch();
            //stopWatch.Start();
            //for (int i = 0; i < 100; i++)
            //{
            //    var evt = new AmountAddEvent(amount, this.State.Balance + amount);
            //    await RaiseEvent(evt, uniqueId: uniqueId).AsTask();
            //}
            //stopWatch.Stop();
            //Console.WriteLine($"100insert:{stopWatch.ElapsedMilliseconds}ms");
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
