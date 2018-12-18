using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core.Exceptions;
using Ray.Core.Messaging;
using Ray.Core.Utils;

namespace Ray.Core.Internal
{
    public abstract class TransactionGrain<K, S, W> : RayGrain<K, S, W>
        where S : class, IState<K>, ICloneable<S>, new()
        where W : IBytesMessage, new()
    {
        public TransactionGrain(ILogger logger) : base(logger)
        {
        }
        protected S BackupState { get; set; }
        protected bool TransactionPending { get; private set; }
        protected long TransactionStartVersion { get; private set; }
        protected DateTime BeginTransactionTime { get; private set; }
        private readonly List<TransactionEventWrapper<K>> transactionEventList = new List<TransactionEventWrapper<K>>();
        protected override async Task RecoveryState()
        {
            await base.RecoveryState();
            BackupState = State.DeepCopy();
        }
        protected async ValueTask BeginTransaction()
        {
            try
            {
                if (TransactionPending)
                {
                    if ((DateTime.UtcNow - BeginTransactionTime).TotalSeconds > ConfigOptions.TransactionTimeoutSeconds)
                    {
                        var rollBackTask = RollbackTransaction();//事务阻赛超过一分钟自动回滚
                        if (!rollBackTask.IsCompleted)
                            await rollBackTask;
                        if (Logger.IsEnabled(LogLevel.Information))
                            Logger.LogInformation(LogEventIds.TransactionGrainTransactionFlow, "Transaction timeout, automatic rollback,type {0} with id {1}", GrainType.FullName, GrainId.ToString());
                    }
                    else
                        throw new RepeatedTransactionException(GrainId.ToString(), GetType());
                }
                var checkTask = StateCheck();
                if (!checkTask.IsCompleted)
                    await checkTask;
                TransactionPending = true;
                TransactionStartVersion = State.Version;
                BeginTransactionTime = DateTime.UtcNow;
                if (Logger.IsEnabled(LogLevel.Information))
                    Logger.LogInformation(LogEventIds.TransactionGrainTransactionFlow, "Begin transaction successfully,type {0} with id {1},transaction start version {2}", GrainType.FullName, GrainId.ToString(), TransactionStartVersion.ToString());
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, "Begin transaction failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
        protected async Task CommitTransaction()
        {
            if (transactionEventList.Count > 0)
            {
                try
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
                        var mqService = GetEventProducer();
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
                    if (Logger.IsEnabled(LogLevel.Information))
                        Logger.LogInformation(LogEventIds.TransactionGrainTransactionFlow, "Commit transaction successfully,type {0} with id {1},transaction start version {2},end version {3}", GrainType.FullName, GrainId.ToString(), TransactionStartVersion.ToString(), State.Version.ToString());
                }
                catch (Exception ex)
                {
                    if (Logger.IsEnabled(LogLevel.Error))
                        Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, "Commit transaction failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                    ExceptionDispatchInfo.Capture(ex).Throw();
                }
            }
            TransactionPending = false;
        }
        protected async ValueTask RollbackTransaction()
        {
            if (TransactionPending)
            {
                try
                {
                    if (BackupState.Version == TransactionStartVersion)
                    {
                        State = BackupState.DeepCopy();
                    }
                    else
                    {
                        await RecoveryState();
                    }
                    transactionEventList.Clear();
                    TransactionPending = false;
                    if (Logger.IsEnabled(LogLevel.Information))
                        Logger.LogInformation(LogEventIds.TransactionGrainTransactionFlow, "Rollback transaction successfully,type {0} with id {1},transaction from version {2} to version {3}", GrainType.FullName, GrainId.ToString(), TransactionStartVersion.ToString(), State.Version.ToString());
                }
                catch (Exception ex)
                {
                    if (Logger.IsEnabled(LogLevel.Error))
                        Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, "Rollback transaction failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                    ExceptionDispatchInfo.Capture(ex).Throw();
                }
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
            if (TransactionPending)
            {
                var ex = new TransactionProcessingSubmitEventException(GrainId.ToString(), GrainType);
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, ex.Message);
                throw ex;
            }
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
                EventApply(BackupState, (IEventBase<K>)Serializer.Deserialize(@event.GetType(), dms));
            }
            BackupState.FullUpdateVersion(@event, GrainType);//更新处理完成的Version
        }
        protected void TransactionRaiseEvent(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            if (!TransactionPending)
            {
                var ex = new UnopenTransactionException(GrainId.ToString(), GrainType, nameof(TransactionRaiseEvent));
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, ex.Message);
                throw ex;
            }
            try
            {
                State.IncrementDoingVersion(GrainType);//标记将要处理的Version
                @event.StateId = GrainId;
                @event.Version = State.Version + 1;
                @event.Timestamp = DateTime.UtcNow;
                transactionEventList.Add(new TransactionEventWrapper<K>(@event, uniqueId, string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey));
                EventApply(State, @event);
                State.UpdateVersion(@event, GrainType);//更新处理完成的Version
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, "type {0} with Id {1},event:{2}", GrainType.FullName, GrainId.ToString(), JsonSerializer.Serialize(@event));
                State.DecrementDoingVersion();//还原doing Version
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
    }
}
