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
        private readonly List<TransactionEventWrapper<K>> EventsInTransactionProcessing = new List<TransactionEventWrapper<K>>();
        protected override async Task RecoveryState()
        {
            await base.RecoveryState();
            BackupState = State.DeepCopy();
        }
        protected async ValueTask BeginTransaction()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Begin transaction with id {0},transaction start state version {1}", GrainId.ToString(), TransactionStartVersion.ToString());
            try
            {
                if (TransactionPending)
                {
                    if ((DateTime.UtcNow - BeginTransactionTime).TotalSeconds > ConfigOptions.TransactionTimeoutSeconds)
                    {
                        var rollBackTask = RollbackTransaction();//事务阻赛超过一分钟自动回滚
                        if (!rollBackTask.IsCompleted)
                            await rollBackTask;
                        if (Logger.IsEnabled(LogLevel.Error))
                            Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, "Transaction timeout, automatic rollback,grain id = {1}", GrainId.ToString());
                    }
                    else
                        throw new RepeatedTransactionException(GrainId.ToString(), GetType());
                }
                var checkTask = TransactionStateCheck();
                if (!checkTask.IsCompleted)
                    await checkTask;
                TransactionPending = true;
                TransactionStartVersion = State.Version;
                BeginTransactionTime = DateTime.UtcNow;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Begin transaction successfully with id {0},transaction start state version {1}", GrainId.ToString(), TransactionStartVersion.ToString());
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(LogEventIds.TransactionGrainTransactionFlow, ex, "Begin transaction failed, grain Id = {1}", GrainId.ToString());
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
        protected async Task CommitTransaction()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Commit transaction with id = {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), EventsInTransactionProcessing.Count.ToString(), TransactionStartVersion.ToString(), State.Version.ToString());
            if (EventsInTransactionProcessing.Count > 0)
            {
                try
                {
                    using (var ms = new PooledMemoryStream())
                    {
                        foreach (var @event in EventsInTransactionProcessing)
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
                    await eventStorageTask.Result.TransactionSaveAsync(EventsInTransactionProcessing);
                    if (SupportAsyncFollow)
                    {
                        var mqService = GetEventProducer();
                        if (!mqService.IsCompleted)
                            await mqService;
                        using (var ms = new PooledMemoryStream())
                        {
                            foreach (var @event in EventsInTransactionProcessing)
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
                        EventsInTransactionProcessing.ForEach(evt => OnRaiseSuccess(evt.Evt, evt.Bytes));
                    }
                    EventsInTransactionProcessing.Clear();
                    var saveSnapshotTask = SaveSnapshotAsync();
                    if (!saveSnapshotTask.IsCompleted)
                        await saveSnapshotTask;
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Commit transaction with id {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), EventsInTransactionProcessing.Count.ToString(), TransactionStartVersion.ToString(), State.Version.ToString());
                }
                catch (Exception ex)
                {
                    if (Logger.IsEnabled(LogLevel.Error))
                        Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, "Commit transaction failed, grain Id = {1}", GrainId.ToString());
                    ExceptionDispatchInfo.Capture(ex).Throw();
                }
            }
            TransactionPending = false;
        }
        protected async ValueTask RollbackTransaction()
        {
            if (TransactionPending)
            {
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Rollback transaction successfully with id = {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), EventsInTransactionProcessing.Count.ToString(), TransactionStartVersion.ToString(), State.Version.ToString());
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
                    EventsInTransactionProcessing.Clear();
                    TransactionPending = false;
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Rollback transaction successfully with id = {0},state version = {1}", GrainId.ToString(), State.Version.ToString());
                }
                catch (Exception ex)
                {
                    if (Logger.IsEnabled(LogLevel.Critical))
                        Logger.LogCritical(LogEventIds.TransactionGrainTransactionFlow, ex, "Rollback transaction failed with Id = {1}", GrainId.ToString());
                    ExceptionDispatchInfo.Capture(ex).Throw();
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask TransactionStateCheck()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Check transaction with id = {0},backup version = {1},state version = {2}", GrainId.ToString(), BackupState.Version, State.Version);
            if (BackupState.Version != State.Version)
            {
                await RecoveryState();
                EventsInTransactionProcessing.Clear();
            }
        }
        protected override async Task<bool> RaiseEvent(IEventBase<K> @event, EventUID uniqueId = null, string hashKey = null)
        {
            if (TransactionPending)
            {
                var ex = new TransactionProcessingSubmitEventException(GrainId.ToString(), GrainType);
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, ex.Message);
                throw ex;
            }
            var checkTask = TransactionStateCheck();
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
        protected void TransactionRaiseEvent(IEventBase<K> @event, EventUID uniqueId = null, string hashKey = null)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start raise event by transaction, grain Id ={0} and state version = {1},event type = {2} ,event = {3},uniqueueId = {4},hashkey = {5}", GrainId.ToString(), State.Version, @event.GetType().FullName, JsonSerializer.Serialize(@event), uniqueId, hashKey);
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
                if (uniqueId == default) uniqueId = EventUID.Empty;
                if (string.IsNullOrEmpty(uniqueId.UID))
                    @event.Timestamp = DateTime.UtcNow;
                else
                    @event.Timestamp = uniqueId.Timestamp;
                EventsInTransactionProcessing.Add(new TransactionEventWrapper<K>(@event, uniqueId.UID, string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey));
                EventApply(State, @event);
                State.UpdateVersion(@event, GrainType);//更新处理完成的Version
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Raise event successfully, grain Id= {0} and state version is {1}}", GrainId.ToString(), State.Version);
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(LogEventIds.TransactionGrainTransactionFlow, ex, "Grain Id = {0},event type = {1} and event = {2}", GrainId.ToString(), @event.GetType().FullName, JsonSerializer.Serialize(@event));
                State.DecrementDoingVersion();//还原doing Version
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
    }
}
