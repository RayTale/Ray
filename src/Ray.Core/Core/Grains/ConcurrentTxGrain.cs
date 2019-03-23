using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Snapshot;

namespace Ray.Core
{
    public abstract class ConcurrentTxGrain<Grain, PrimaryKey, SnapshotType> : TxGrain<Grain, PrimaryKey, SnapshotType>
        where SnapshotType : class, ICloneable<SnapshotType>, new()
    {
        public long defaultTransactionId = 0;
        public ConcurrentTxGrain() : base()
        {
        }
        protected IMpscChannel<ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>> ConcurrentChannel { get; private set; }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            ConcurrentChannel = ServiceProvider.GetService<IMpscChannel<ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>>>();
            ConcurrentChannel.BindConsumer(BatchInputProcessing);
        }
        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            ConcurrentChannel.Complete();
        }
        protected async ValueTask ConcurrentRaiseEvent(
            Func<Snapshot<PrimaryKey, SnapshotType>, Func<IEvent, EventUID, Task>, Task> handler,
            Func<bool, ValueTask> completedHandler,
            Action<Exception> exceptionHandler)
        {
            var writeTask = ConcurrentChannel.WriteAsync(new ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>(handler, completedHandler, exceptionHandler));
            if (!writeTask.IsCompletedSuccessfully)
                await writeTask;
            if (!writeTask.Result)
            {
                var ex = new ChannelUnavailabilityException(GrainId.ToString(), GrainType);
                Logger.LogError(ex, ex.Message);
                throw ex;
            }
        }
        protected async Task<bool> ConcurrentRaiseEvent(Func<Snapshot<PrimaryKey, SnapshotType>, Func<IEvent, EventUID, Task>, Task> handler)
        {
            var taskSource = new TaskCompletionSource<bool>();
            var task = ConcurrentRaiseEvent(handler, isOk =>
            {
                taskSource.TrySetResult(isOk);
                return Consts.ValueTaskDone;
            }, ex =>
            {
                taskSource.TrySetException(ex);
            });
            if (!task.IsCompletedSuccessfully)
                await task;
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
        protected async Task<bool> ConcurrentRaiseEvent(IEvent evt, EventUID uniqueId = null)
        {
            var taskSource = new TaskCompletionSource<bool>();
            var task = ConcurrentRaiseEvent(async (state, eventFunc) =>
            {
                await eventFunc(evt, uniqueId);
            }, isOk =>
            {
                taskSource.TrySetResult(isOk);
                return Consts.ValueTaskDone;
            }, ex =>
            {
                taskSource.TrySetException(ex);
            });
            if (!task.IsCompletedSuccessfully)
                await task;
            return await taskSource.Task;
        }
        protected virtual ValueTask OnBatchInputProcessed() => Consts.ValueTaskDone;
        private async Task BatchInputProcessing(List<ConcurrentTransport<Snapshot<PrimaryKey, SnapshotType>>> inputs)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start batch event processing with id = {0},state version = {1},the number of events = {2}", GrainId.ToString(), CurrentTransactionStartVersion, inputs.Count.ToString());
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
                    if (input.Executed)
                    {
                        var completeTask = input.CompletedHandler(true);
                        if (!completeTask.IsCompletedSuccessfully)
                            await completeTask;
                    }
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
                    inputs.ForEach(input => input.ExceptionHandler(ex));
                }
            }
            var onCompletedTask = OnBatchInputProcessed();
            if (!onCompletedTask.IsCompletedSuccessfully)
                await onCompletedTask;
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Batch events have been processed with id = {0},state version = {1},the number of events = {2}", GrainId.ToString(), CurrentTransactionStartVersion, inputs.Count.ToString());
            async Task ReTry()
            {
                foreach (var input in inputs)
                {
                    try
                    {
                        await input.Handler(Snapshot, async (evt, uniqueId) =>
                        {
                            var result = await RaiseEvent(evt, uniqueId);
                            var completeTask = input.CompletedHandler(result);
                            if (!completeTask.IsCompletedSuccessfully)
                                await completeTask;
                        });
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex, ex.Message);
                        input.ExceptionHandler(ex);
                    }
                }
            }
        }
    }
}
