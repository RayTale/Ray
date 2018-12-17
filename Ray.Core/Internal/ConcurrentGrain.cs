using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Exceptions;
using Ray.Core.Messaging.Channels;

namespace Ray.Core.Internal
{
    public abstract  class ConcurrentGrain<K, S, W> : TransactionGrain<K, S, W>
        where S : class, IState<K>, ICloneable<S>, new()
        where W : IMessageWrapper, new()
    {
        public ConcurrentGrain(ILogger logger) : base(logger)
        {
        }
        protected IMpscChannel<ConcurrentWrapper<K, S>> MpscChannel { get; private set; }
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            MpscChannel = ServiceProvider.GetService<IMpscChannelFactory<K, ConcurrentWrapper<K, S>>>().Create(Logger, GrainId, BatchInputProcessing, ConfigOptions.MaxSizeOfPerBatch);
        }
        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            MpscChannel.Complete();
        }
        protected async ValueTask ConcurrentRaiseEvent(Func<S, Func<IEventBase<K>, string, string, Task>, Task> handler, Func<bool, ValueTask> completedHandler, Action<Exception> exceptionHandler)
        {
            var writeTask = MpscChannel.WriteAsync(new ConcurrentWrapper<K, S>(handler, completedHandler, exceptionHandler));
            if (!writeTask.IsCompleted)
                await writeTask;
            if (!writeTask.Result)
            {
                var ex = new ChannelUnavailabilityException(GrainId.ToString(), GrainType);
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainCurrentInput, ex, ex.Message);
                throw ex;
            }
        }
        protected virtual ValueTask OnBatchInputCompleted()
        {
            return new ValueTask(Task.CompletedTask);
        }
        private async Task BatchInputProcessing(List<ConcurrentWrapper<K, S>> inputs)
        {
            if (Logger.IsEnabled(LogLevel.Information))
                Logger.LogInformation(LogEventIds.TransactionGrainCurrentInput, "Start batch event processing,type {0} with id {1},state version {2},the number of events is {3}", GrainType.FullName, GrainId.ToString(), TransactionStartVersion, inputs.Count.ToString());
            var beginTask = BeginTransaction();
            if (!beginTask.IsCompleted)
                await beginTask;
            try
            {
                foreach (var input in inputs)
                {
                    await input.Handler(State, (evt, uniqueId, hashKey) =>
                    {
                        TransactionRaiseEvent(evt, uniqueId, hashKey);
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
                        if (!completeTask.IsCompleted)
                            await completeTask;
                    }
                }
            }
            catch
            {
                try
                {
                    var rollBackTask = RollbackTransaction();
                    if (!rollBackTask.IsCompleted)
                        await rollBackTask;
                    await ReTry();
                }
                catch (Exception ex)
                {
                    inputs.ForEach(input => input.ExceptionHandler(ex));
                }
            }
            var onCompletedTask = OnBatchInputCompleted();
            if (!onCompletedTask.IsCompleted)
                await onCompletedTask;
            if (Logger.IsEnabled(LogLevel.Information))
                Logger.LogInformation(LogEventIds.TransactionGrainCurrentInput, "Batch events have been processed,type {0} with id {1},state version {2},the number of events is {3}", GrainType.FullName, GrainId.ToString(), TransactionStartVersion, inputs.Count.ToString());
            async Task ReTry()
            {
                foreach (var input in inputs)
                {
                    try
                    {
                        await input.Handler(State, async (evt, uniqueId, hashKey) =>
                        {
                            var result = await RaiseEvent(evt, uniqueId, hashKey);
                            var completeTask = input.CompletedHandler(result);
                            if (!completeTask.IsCompleted)
                                await completeTask;
                        });
                    }
                    catch (Exception ex)
                    {
                        input.ExceptionHandler(ex);
                    }
                }
            }
        }
    }
}
