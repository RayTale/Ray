using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Snapshot;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core
{
    public abstract class ConcurrentTxGrain<PrimaryKey, SnapshotType> : TxGrain<PrimaryKey, SnapshotType>
        where SnapshotType : class, ICloneable<SnapshotType>, new()
    {
        public long defaultTransactionId = 0;
        protected IMpscChannel<ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>> ConcurrentChannel { get; private set; }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            ConcurrentChannel = ServiceProvider.GetService<IMpscChannel<ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>>>();
            ConcurrentChannel.BindConsumer(ConcurrentExecuter);
        }
        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            ConcurrentChannel.Complete();
        }
        protected async Task<bool> ConcurrentRaiseEvent(
            Func<Snapshot<PrimaryKey, SnapshotType>, Func<IEvent, EventUID, Task>, Task> handler)
        {
            var taskSource = new TaskCompletionSource<bool>();
            var writeTask = ConcurrentChannel.WriteAsync(new ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>(defaultTransactionId, handler, taskSource));
            if (!writeTask.IsCompletedSuccessfully)
                await writeTask;
            if (!writeTask.Result)
            {
                var ex = new ChannelUnavailabilityException(GrainId.ToString(), GrainType);
                Logger.LogError(ex, ex.Message);
                throw ex;
            }
            return await taskSource.Task;
        }
        protected async Task<bool> ConcurrentTxRaiseEvent(
            long transactionId,
            Func<Snapshot<PrimaryKey, SnapshotType>, Func<IEvent, EventUID, Task>, Task> handler)
        {
            var taskSource = new TaskCompletionSource<bool>();
            if (transactionId <= 0)
                throw new TxIdException();
            var writeTask = ConcurrentChannel.WriteAsync(new ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>(transactionId, handler, taskSource));
            if (!writeTask.IsCompletedSuccessfully)
                await writeTask;
            if (!writeTask.Result)
            {
                var ex = new ChannelUnavailabilityException(GrainId.ToString(), GrainType);
                Logger.LogError(ex, ex.Message);
                throw ex;
            }
            return await taskSource.Task;
        }
        /// <summary>
        /// 不依赖当前状态的的事件的并发处理
        /// 如果事件的产生依赖当前状态，请使用<see cref="ConcurrentRaiseEvent(Func{Snapshot{PrimaryKey, SnapshotType}, Func{IEvent, EventUID, Task}, Task}, Func{bool, ValueTask}, Action{Exception})"/>
        /// </summary>
        /// <param name="event">不依赖当前状态的事件</param>
        /// <param name="uniqueId">幂等性判定值</param>
        /// <param name="hashKey">消息异步分发的唯一hash的key</param>
        /// <returns></returns>
        protected Task<bool> ConcurrentRaiseEvent(IEvent evt, EventUID uniqueId = null)
        {
            return ConcurrentRaiseEvent(async (snapshot, eventFunc) =>
            {
                await eventFunc(evt, uniqueId);
            });
        }
        protected virtual ValueTask OnConcurrentExecuted() => Consts.ValueTaskDone;
        private async Task ConcurrentExecuter(List<ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>> inputs)
        {
            var autoTransactionList = new List<ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>>();
            foreach (var input in inputs)
            {
                if (input.TransactionId == defaultTransactionId)
                {
                    autoTransactionList.Add(input);
                }
                else
                {
                    try
                    {
                        await input.Handler(Snapshot, async (evt, uniqueId) =>
                         {
                             await TxRaiseEvent(input.TransactionId, evt, uniqueId);
                             input.Executed = true;
                         });
                        input.Completed(input.Executed);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex, ex.Message);
                        input.Exception(ex);
                    }
                }
            }
            if (autoTransactionList.Count > 0)
            {
                await AutoTransactionExcuter(autoTransactionList);
            }
        }
        private async Task AutoTransactionExcuter(List<ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>> inputs)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("AutoTransaction: {0}->{1}->{2}", GrainType.FullName, GrainId.ToString(), inputs.Count.ToString());
            await BeginTransaction(defaultTransactionId);
            try
            {
                foreach (var input in inputs)
                {
                    await input.Handler(Snapshot, (evt, uniqueId) =>
                    {
                        TxRaiseEvent(evt, uniqueId);
                        input.Executed = true;
                        return Task.CompletedTask;
                    });
                }
                await CommitTransaction(defaultTransactionId);
                await FinishTransaction(defaultTransactionId);
                foreach (var input in inputs)
                {
                    input.Completed(input.Executed);
                }
            }
            catch (Exception batchEx)
            {
                Logger.LogError(batchEx, batchEx.Message);
                try
                {
                    await RollbackTransaction(defaultTransactionId);
                    await ReTry();
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, ex.Message);
                    inputs.ForEach(input => input.Exception(ex));
                }
            }
            var onCompletedTask = OnConcurrentExecuted();
            if (!onCompletedTask.IsCompletedSuccessfully)
                await onCompletedTask;
            async Task ReTry()
            {
                foreach (var input in inputs)
                {
                    try
                    {
                        await input.Handler(Snapshot, async (evt, uniqueId) =>
                        {
                            var result = await RaiseEvent(evt, uniqueId);
                            input.Completed(result);
                        });
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex, ex.Message);
                        input.Exception(ex);
                    }
                }
            }
        }
    }
}
