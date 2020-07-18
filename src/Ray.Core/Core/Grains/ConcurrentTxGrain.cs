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
    public abstract class ConcurrentTxGrain<PrimaryKey, SnapshotType> : TxGrain<PrimaryKey, SnapshotType>
        where SnapshotType : class, ICloneable<SnapshotType>, new()
    {
        public string defaultTransactionId = string.Empty;

        protected IMpscChannel<EventTaskComplexBox<Snapshot<PrimaryKey, SnapshotType>>> ConcurrentChannel { get; private set; }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            this.ConcurrentChannel = this.ServiceProvider.GetService<IMpscChannel<EventTaskComplexBox<Snapshot<PrimaryKey, SnapshotType>>>>();
            this.ConcurrentChannel.BindConsumer(this.ConcurrentExecuter);
        }

        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            this.ConcurrentChannel.Dispose();
        }

        protected async Task<bool> ConcurrentRaiseEvent(
            Func<Snapshot<PrimaryKey, SnapshotType>, Func<IEvent, EventUID, Task>, Task> handler)
        {
            var taskSource = new TaskCompletionSource<bool>();
            var writeTask = this.ConcurrentChannel.WriteAsync(new EventTaskComplexBox<Snapshot<PrimaryKey, SnapshotType>>(this.defaultTransactionId, handler, taskSource));
            if (!writeTask.IsCompletedSuccessfully)
            {
                await writeTask;
            }

            if (!writeTask.Result)
            {
                var ex = new ChannelUnavailabilityException(this.GrainId.ToString(), this.GrainType);
                this.Logger.LogError(ex, ex.Message);
                throw ex;
            }

            return await taskSource.Task;
        }

        protected async Task<bool> ConcurrentTxRaiseEvent(
            string transactionId,
            Func<Snapshot<PrimaryKey, SnapshotType>, Func<IEvent, EventUID, Task>, Task> handler)
        {
            var taskSource = new TaskCompletionSource<bool>();
            if (transactionId == default)
            {
                throw new TxIdException();
            }

            var writeTask = this.ConcurrentChannel.WriteAsync(new EventTaskComplexBox<Snapshot<PrimaryKey, SnapshotType>>(transactionId, handler, taskSource));
            if (!writeTask.IsCompletedSuccessfully)
            {
                await writeTask;
            }

            if (!writeTask.Result)
            {
                var ex = new ChannelUnavailabilityException(this.GrainId.ToString(), this.GrainType);
                this.Logger.LogError(ex, ex.Message);
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
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        protected Task<bool> ConcurrentRaiseEvent(IEvent evt, EventUID uniqueId = null)
        {
            return this.ConcurrentRaiseEvent(async (snapshot, eventFunc) =>
            {
                await eventFunc(evt, uniqueId);
            });
        }

        protected virtual ValueTask OnConcurrentExecuted() => Consts.ValueTaskDone;

        private async Task ConcurrentExecuter(List<EventTaskComplexBox<Snapshot<PrimaryKey, SnapshotType>>> inputs)
        {
            var autoTransactionList = new List<EventTaskComplexBox<Snapshot<PrimaryKey, SnapshotType>>>();
            foreach (var input in inputs)
            {
                if (input.TransactionId == this.defaultTransactionId)
                {
                    autoTransactionList.Add(input);
                }
                else
                {
                    try
                    {
                        await input.Handler(this.Snapshot, async (evt, uniqueId) =>
                         {
                             await this.TxRaiseEvent(input.TransactionId, evt, uniqueId);
                             input.Executed = true;
                         });
                        input.Completed(input.Executed);
                    }
                    catch (Exception ex)
                    {
                        this.Logger.LogError(ex, ex.Message);
                        input.Exception(ex);
                    }
                }
            }

            if (autoTransactionList.Count > 0)
            {
                await this.AutoTransactionExcuter(autoTransactionList);
            }
        }

        private async Task AutoTransactionExcuter(List<EventTaskComplexBox<Snapshot<PrimaryKey, SnapshotType>>> inputs)
        {
            if (this.Logger.IsEnabled(LogLevel.Trace))
            {
                this.Logger.LogTrace("AutoTransaction: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), inputs.Count.ToString());
            }

            await this.BeginTransaction(this.defaultTransactionId);
            try
            {
                foreach (var input in inputs)
                {
                    await input.Handler(this.Snapshot, (evt, uniqueId) =>
                    {
                        this.TxRaiseEvent(evt, uniqueId);
                        input.Executed = true;
                        return Task.CompletedTask;
                    });
                }

                await this.CommitTransaction(this.defaultTransactionId);
                await this.FinishTransaction(this.defaultTransactionId);
                foreach (var input in inputs)
                {
                    input.Completed(input.Executed);
                }
            }
            catch (Exception batchEx)
            {
                this.Logger.LogError(batchEx, batchEx.Message);
                try
                {
                    await this.RollbackTransaction(this.defaultTransactionId);
                    await ReTry();
                }
                catch (Exception ex)
                {
                    this.Logger.LogError(ex, ex.Message);
                    inputs.ForEach(input => input.Exception(ex));
                }
            }

            var onCompletedTask = this.OnConcurrentExecuted();
            if (!onCompletedTask.IsCompletedSuccessfully)
            {
                await onCompletedTask;
            }

            async Task ReTry()
            {
                foreach (var input in inputs)
                {
                    try
                    {
                        await input.Handler(this.Snapshot, async (evt, uniqueId) =>
                        {
                            var result = await this.RaiseEvent(evt, uniqueId);
                            input.Completed(result);
                        });
                    }
                    catch (Exception ex)
                    {
                        this.Logger.LogError(ex, ex.Message);
                        input.Exception(ex);
                    }
                }
            }
        }
    }
}
