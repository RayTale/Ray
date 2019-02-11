using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Logging;
using Ray.Core.State;

namespace Ray.Core
{
    public abstract class ConcurrentGrain<Children, PrimaryKey, State> : TransactionGrain<Children, PrimaryKey, State>
        where State : class, ICloneable<State>, new()
    {
        public ConcurrentGrain(ILogger logger) : base(logger)
        {
        }
        protected IMpscChannel<ConcurrentTransport<Snapshot<PrimaryKey, State>>> ConcurrentChannel { get; private set; }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            ConcurrentChannel = ServiceProvider.GetService<IMpscChannel<ConcurrentTransport<Snapshot<PrimaryKey, State>>>>();
            ConcurrentChannel.BindConsumer(BatchInputProcessing).ActiveConsumer();
        }
        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            ConcurrentChannel.Complete();
        }
        protected async ValueTask ConcurrentRaiseEvent(
            Func<Snapshot<PrimaryKey, State>, Func<IEvent, EventUID, Task>, Task> handler,
            Func<bool, ValueTask> completedHandler,
            Action<Exception> exceptionHandler)
        {
            var writeTask = ConcurrentChannel.WriteAsync(new ConcurrentTransport<Snapshot<PrimaryKey, State>>(handler, completedHandler, exceptionHandler));
            if (!writeTask.IsCompletedSuccessfully)
                await writeTask;
            if (!writeTask.Result)
            {
                var ex = new ChannelUnavailabilityException(GrainId.ToString(), GrainType);
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainCurrentInput, ex, ex.Message);
                throw ex;
            }
        }
        protected async Task<bool> ConcurrentRaiseEvent(Func<Snapshot<PrimaryKey, State>, Func<IEvent, EventUID, Task>, Task> handler)
        {
            var taskSource = new TaskCompletionSource<bool>();
            var task = ConcurrentRaiseEvent(handler, isOk =>
            {
                taskSource.TrySetResult(isOk);
                return new ValueTask();
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
        /// 如果事件的产生依赖当前状态，请使用<see cref="ConcurrentRaiseEvent(Func{Snapshot{PrimaryKey, State}, Func{IEvent, EventUID, Task}, Task}, Func{bool, ValueTask}, Action{Exception})"/>
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
                return new ValueTask();
            }, ex =>
            {
                taskSource.TrySetException(ex);
            });
            if (!task.IsCompletedSuccessfully)
                await task;
            return await taskSource.Task;
        }
        protected virtual ValueTask OnBatchInputProcessed() => Consts.ValueTaskDone;
        private async Task BatchInputProcessing(List<ConcurrentTransport<Snapshot<PrimaryKey, State>>> inputs)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.TransactionGrainCurrentProcessing, "Start batch event processing with id = {0},state version = {1},the number of events = {2}", GrainId.ToString(), TransactionStartVersion, inputs.Count.ToString());
            var beginTask = BeginTransaction();
            if (!beginTask.IsCompletedSuccessfully)
                await beginTask;
            try
            {
                foreach (var input in inputs)
                {
                    await input.Handler(Snapshot, (evt, uniqueId) =>
                    {
                        TransactionRaiseEvent(evt, uniqueId);
                        input.Executed = true;
                        return Task.CompletedTask;
                    });
                }
                await CommitTransaction();
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
                if (Logger.IsEnabled(LogLevel.Information))
                    Logger.LogInformation(LogEventIds.TransactionGrainCurrentProcessing, batchEx, batchEx.Message);
                try
                {
                    var rollBackTask = RollbackTransaction();
                    if (!rollBackTask.IsCompletedSuccessfully)
                        await rollBackTask;
                    await ReTry();
                }
                catch (Exception ex)
                {
                    if (Logger.IsEnabled(LogLevel.Error))
                        Logger.LogError(LogEventIds.TransactionGrainCurrentProcessing, ex, ex.Message);
                    inputs.ForEach(input => input.ExceptionHandler(ex));
                }
            }
            var onCompletedTask = OnBatchInputProcessed();
            if (!onCompletedTask.IsCompletedSuccessfully)
                await onCompletedTask;
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.TransactionGrainCurrentProcessing, "Batch events have been processed with id = {0},state version = {1},the number of events = {2}", GrainId.ToString(), TransactionStartVersion, inputs.Count.ToString());
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
                        if (Logger.IsEnabled(LogLevel.Error))
                            Logger.LogError(LogEventIds.TransactionGrainCurrentProcessing, ex, ex.Message);
                        input.ExceptionHandler(ex);
                    }
                }
            }
        }
    }
}
