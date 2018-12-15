using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Exceptions;
using Ray.Core.Messaging;
using Ray.Core.Messaging.Channels;
using Ray.Core.Utils;

namespace Ray.Core.Internal
{
    public abstract class TransactionGrain<K, S, W> : RayGrain<K, S, W>
        where S : class, IState<K>, ITransactionable<S>, new()
        where W : IMessageWrapper, new()
    {
        public TransactionGrain(ILogger logger) : base(logger)
        {
        }
        protected S BackupState { get; set; }
        private IMpscChannel<EventWrapper<K>, bool> mpscChannel;
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            mpscChannel = ServiceProvider.GetService<IMpscChannelFactory<K, EventWrapper<K>, bool>>().Create(Logger, GrainId, BatchInputProcessing, ConfigOptions.MaxSizeOfPerBatch);
        }
        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            mpscChannel.Complete();
        }
        protected bool transactionPending = false;
        private long transactionStartVersion;
        private DateTime beginTransactionTime;
        private readonly List<EventStorageWrapper<K>> transactionEventList = new List<EventStorageWrapper<K>>();
        protected override async Task RecoveryState()
        {
            await base.RecoveryState();
            BackupState = State.DeepCopy();
        }
        protected async ValueTask BeginTransaction()
        {
            if (transactionPending)
            {
                if ((DateTime.UtcNow - beginTransactionTime).TotalSeconds > ConfigOptions.TransactionTimeoutSeconds)
                {
                    var rollBackTask = RollbackTransaction();//事务阻赛超过一分钟自动回滚
                    if (!rollBackTask.IsCompleted)
                        await rollBackTask;
                }
                else
                    throw new RepeatedTransactionException(GrainId.ToString(), GetType());
            }
            var checkTask = StateCheck();
            if (!checkTask.IsCompleted)
                await checkTask;
            transactionPending = true;
            transactionStartVersion = State.Version;
            beginTransactionTime = DateTime.UtcNow;
        }
        protected async Task CommitTransaction()
        {
            if (transactionEventList.Count > 0)
            {
                using (var ms = new PooledMemoryStream())
                {
                    foreach (var @event in transactionEventList)
                    {
                        Serializer.Serialize(ms, @event.Evt);
                        @event.Bytes = ms.ToArray();
                        ms.Position = 0;
                        ms.SetLength(0);
                    }
                }
                var eventStorageTask = GetEventStorage();
                if (!eventStorageTask.IsCompleted)
                    await eventStorageTask;
                await eventStorageTask.Result.TransactionSaveAsync(transactionEventList);
                if (SupportAsyncFollow)
                {
                    var mqService = GetMQService();
                    if (!mqService.IsCompleted)
                        await mqService;
                    using (var ms = new PooledMemoryStream())
                    {
                        foreach (var @event in transactionEventList)
                        {
                            var data = new W
                            {
                                TypeName = @event.Evt.GetType().FullName,
                                Bytes = @event.Bytes
                            };
                            Serializer.Serialize(ms, data);
                            var publishTask = mqService.Result.Publish(ms.ToArray(), @event.HashKey);
                            if (!publishTask.IsCompleted)
                                await publishTask;
                            OnRaiseSuccess(@event.Evt, @event.Bytes);
                            ms.Position = 0;
                            ms.SetLength(0);
                        }
                    }
                }
                else
                {
                    transactionEventList.ForEach(evt => OnRaiseSuccess(evt.Evt, evt.Bytes));
                }
                transactionEventList.Clear();
                var saveSnapshotTask = SaveSnapshotAsync();
                if (!saveSnapshotTask.IsCompleted)
                    await saveSnapshotTask;
            }
            transactionPending = false;
        }
        protected async ValueTask RollbackTransaction()
        {
            if (transactionPending)
            {
                if (BackupState.Version == transactionStartVersion)
                {
                    State = BackupState.DeepCopy();
                }
                else
                {
                    await RecoveryState();
                }
                transactionEventList.Clear();
                transactionPending = false;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask StateCheck()
        {
            if (BackupState.Version != State.Version)
            {
                await RecoveryState();
                transactionEventList.Clear();
            }
        }
        protected override async Task<bool> RaiseEvent(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            if (transactionPending)
                throw new Exception("The transaction has been opened,Please use Transaction().");
            var checkTask = StateCheck();
            if (!checkTask.IsCompleted)
                await checkTask;
            return await base.RaiseEvent(@event, uniqueId, hashKey);
        }
        /// <summary>
        /// 防止对象在State和BackupState中互相干扰，所以反序列化一个全新的Event对象给BackupState
        /// </summary>
        /// <param name="event">事件本体</param>
        /// <param name="bytes">事件序列化之后的二进制数据</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void OnRaiseSuccess(IEventBase<K> @event, byte[] bytes)
        {
            using (var dms = new MemoryStream(bytes))
            {
                Apply(BackupState, (IEventBase<K>)Serializer.Deserialize(@event.GetType(), dms));
            }
            BackupState.FullUpdateVersion(@event, GrainType);//更新处理完成的Version
        }
        protected void Transaction(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            if (!transactionPending)
                throw new Exception("Unopened transaction,Please open the transaction first.");
            try
            {
                State.IncrementDoingVersion(GrainType);//标记将要处理的Version
                @event.StateId = GrainId;
                @event.Version = State.Version + 1;
                @event.Timestamp = DateTime.UtcNow;
                transactionEventList.Add(new EventStorageWrapper<K>(@event, uniqueId, string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey));
                Apply(State, @event);
                State.UpdateVersion(@event, GrainType);//更新处理完成的Version
            }
            catch (Exception ex)
            {
                State.DecrementDoingVersion();//还原doing Version
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
        protected Task<bool> ConcurrentRaiseEvent(IEventBase<K> evt, string uniqueId = null)
        {
            return mpscChannel.WriteAsync(new EventWrapper<K>(evt, uniqueId));
        }
        protected virtual ValueTask OnBatchInputCompleted()
        {
            return new ValueTask(Task.CompletedTask);
        }
        private async Task BatchInputProcessing(List<MessageTaskWrapper<EventWrapper<K>, bool>> events)
        {
            var beginTask = BeginTransaction();
            if (!beginTask.IsCompleted)
                await beginTask;
            try
            {
                foreach (var value in events)
                {
                    Transaction(value.Value.Value, value.Value.UniqueId);
                }
                await CommitTransaction();
                events.ForEach(evt => evt.TaskSource.SetResult(true));
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
                catch (Exception e)
                {
                    events.ForEach(evt => evt.TaskSource.TrySetException(e));
                }
            }
            var onCompletedTask = OnBatchInputCompleted();
            if (!onCompletedTask.IsCompleted)
                await onCompletedTask;

            async Task ReTry()
            {
                foreach (var evt in events)
                {
                    try
                    {
                        evt.TaskSource.TrySetResult(await RaiseEvent(evt.Value.Value, evt.Value.UniqueId));
                    }
                    catch (Exception e)
                    {
                        evt.TaskSource.TrySetException(e);
                    }
                }
            }
        }
    }
}
