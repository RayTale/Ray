using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Logging;
using Ray.Core.State;
using Ray.Core.Utils;

namespace Ray.Core
{
    public abstract class TransactionGrain<Children, PrimaryKey, State> : MainGrain<Children, PrimaryKey, State>
        where State : class, ICloneable<State>, new()
    {
        public TransactionGrain(ILogger logger) : base(logger)
        {
        }
        protected Snapshot<PrimaryKey, State> BackupSnapshot { get; set; }
        protected bool TransactionPending { get; private set; }
        protected long TransactionStartVersion { get; private set; }
        protected DateTimeOffset BeginTransactionTime { get; private set; }
        private readonly List<TransactionTransport<PrimaryKey>> EventsInTransactionProcessing = new List<TransactionTransport<PrimaryKey>>();
        protected override async Task RecoveryState()
        {
            await base.RecoveryState();
            BackupSnapshot = new Snapshot<PrimaryKey, State>(GrainId)
            {
                Base = Snapshot.Base.Clone(),
                State = Snapshot.State.Clone()
            };
        }
        protected async ValueTask BeginTransaction()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Begin transaction with id {0},transaction start state version {1}", GrainId.ToString(), TransactionStartVersion.ToString());
            try
            {
                if (TransactionPending)
                {
                    if ((DateTimeOffset.UtcNow - BeginTransactionTime).TotalSeconds > CoreOptions.TransactionTimeoutSeconds)
                    {
                        var rollBackTask = RollbackTransaction();//事务阻赛超过一分钟自动回滚
                        if (!rollBackTask.IsCompletedSuccessfully)
                            await rollBackTask;
                        if (Logger.IsEnabled(LogLevel.Error))
                            Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, "Transaction timeout, automatic rollback,grain id = {1}", GrainId.ToString());
                    }
                    else
                        throw new RepeatedTransactionException(GrainId.ToString(), GetType());
                }
                var checkTask = TransactionStateCheck();
                if (!checkTask.IsCompletedSuccessfully)
                    await checkTask;
                TransactionPending = true;
                TransactionStartVersion = Snapshot.Base.Version;
                BeginTransactionTime = DateTimeOffset.UtcNow;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Begin transaction successfully with id {0},transaction start state version {1}", GrainId.ToString(), TransactionStartVersion.ToString());
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(LogEventIds.TransactionGrainTransactionFlow, ex, "Begin transaction failed, grain Id = {1}", GrainId.ToString());
                throw;
            }
        }
        protected async Task CommitTransaction()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Commit transaction with id = {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), EventsInTransactionProcessing.Count.ToString(), TransactionStartVersion.ToString(), Snapshot.Base.Version.ToString());
            if (EventsInTransactionProcessing.Count > 0)
            {
                try
                {
                    using (var ms = new PooledMemoryStream())
                    {
                        foreach (var transport in EventsInTransactionProcessing)
                        {
                            Serializer.Serialize(ms, transport.FullyEvent.Event);
                            transport.BytesTransport = new BytesTransport
                            {
                                EventType = transport.FullyEvent.Event.GetType().FullName,
                                ActorId = GrainId,
                                EventBytes = ms.ToArray(),
                                BaseBytes = transport.FullyEvent.Base.GetBytes()
                            };
                            ms.Position = 0;
                            ms.SetLength(0);
                        }
                    }
                    await EventStorage.TransactionBatchAppend(EventsInTransactionProcessing);
                    if (SupportFollow)
                    {
                        try
                        {
                            foreach (var transport in EventsInTransactionProcessing)
                            {
                                var publishTask = EventBusProducer.Publish(transport.BytesTransport.GetBytes(), transport.HashKey);
                                if (!publishTask.IsCompletedSuccessfully)
                                    await publishTask;
                                var task = OnRaiseSuccessed(transport.FullyEvent, transport.BytesTransport);
                                if (!task.IsCompletedSuccessfully)
                                    await task;
                            }
                        }
                        catch (Exception ex)
                        {
                            if (Logger.IsEnabled(LogLevel.Error))
                                Logger.LogError(LogEventIds.GrainRaiseEvent, ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), Snapshot.Base.Version);
                        }
                    }
                    else
                    {
                        foreach (var transport in EventsInTransactionProcessing)
                        {
                            var task = OnRaiseSuccessed(transport.FullyEvent, transport.BytesTransport);
                            if (!task.IsCompletedSuccessfully)
                                await task;
                        }
                    }
                    EventsInTransactionProcessing.Clear();
                    var saveSnapshotTask = SaveSnapshotAsync();
                    if (!saveSnapshotTask.IsCompletedSuccessfully)
                        await saveSnapshotTask;
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Commit transaction with id {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), EventsInTransactionProcessing.Count.ToString(), TransactionStartVersion.ToString(), Snapshot.Base.Version.ToString());
                }
                catch (Exception ex)
                {
                    if (Logger.IsEnabled(LogLevel.Error))
                        Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, "Commit transaction failed, grain Id = {1}", GrainId.ToString());
                    throw;
                }
            }
            TransactionPending = false;
        }
        protected async ValueTask RollbackTransaction()
        {
            if (TransactionPending)
            {
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Rollback transaction successfully with id = {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), EventsInTransactionProcessing.Count.ToString(), TransactionStartVersion.ToString(), Snapshot.Base.Version.ToString());
                try
                {
                    if (BackupSnapshot.Base.Version == TransactionStartVersion)
                    {
                        Snapshot = new Snapshot<PrimaryKey, State>(GrainId)
                        {
                            Base = BackupSnapshot.Base.Clone(),
                            State = BackupSnapshot.State.Clone()
                        };
                    }
                    else
                    {
                        await RecoveryState();
                    }
                    EventsInTransactionProcessing.Clear();
                    TransactionPending = false;
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Rollback transaction successfully with id = {0},state version = {1}", GrainId.ToString(), Snapshot.Base.Version.ToString());
                }
                catch (Exception ex)
                {
                    if (Logger.IsEnabled(LogLevel.Critical))
                        Logger.LogCritical(LogEventIds.TransactionGrainTransactionFlow, ex, "Rollback transaction failed with Id = {1}", GrainId.ToString());
                    throw;
                }
            }
        }
        private async ValueTask TransactionStateCheck()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Check transaction with id = {0},backup version = {1},state version = {2}", GrainId.ToString(), BackupSnapshot.Base.Version, Snapshot.Base.Version);
            if (BackupSnapshot.Base.Version != Snapshot.Base.Version)
            {
                await RecoveryState();
                EventsInTransactionProcessing.Clear();
            }
        }
        protected override async Task<bool> RaiseEvent(IEvent @event, EventUID uniqueId = null)
        {
            if (TransactionPending)
            {
                var ex = new TransactionProcessingSubmitEventException(GrainId.ToString(), GrainType);
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, ex.Message);
                throw ex;
            }
            var checkTask = TransactionStateCheck();
            if (!checkTask.IsCompletedSuccessfully)
                await checkTask;
            return await base.RaiseEvent(@event, uniqueId);
        }
        /// <summary>
        /// 防止对象在State和BackupState中互相干扰，所以反序列化一个全新的Event对象给BackupState
        /// </summary>
        /// <param name="fullyEvent">事件本体</param>
        /// <param name="bytes">事件序列化之后的二进制数据</param>
        protected override ValueTask OnRaiseSuccessed(IFullyEvent<PrimaryKey> fullyEvent, BytesTransport bytesTransport)
        {
            using (var ms = new MemoryStream(bytesTransport.EventBytes))
            {
                var newEvent = new FullyEvent<PrimaryKey>
                {
                    Event = Serializer.Deserialize(fullyEvent.Event.GetType(), ms) as IEvent,
                    Base = EventBase.FromBytes(bytesTransport.BaseBytes)
                };
                EventApply(BackupSnapshot, newEvent);
                BackupSnapshot.Base.FullUpdateVersion(newEvent.Base, GrainType);//更新处理完成的Version
            }
            //父级涉及状态归档
            return base.OnRaiseSuccessed(fullyEvent, bytesTransport);
        }
        protected void TransactionRaiseEvent(IEvent @event, EventUID uniqueId = null, string hashKey = null)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start raise event by transaction, grain Id ={0} and state version = {1},event type = {2} ,event = {3},uniqueueId = {4},hashkey = {5}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, JsonSerializer.Serialize(@event), uniqueId, hashKey);
            if (!TransactionPending)
            {
                var ex = new UnopenTransactionException(GrainId.ToString(), GrainType, nameof(TransactionRaiseEvent));
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.TransactionGrainTransactionFlow, ex, ex.Message);
                throw ex;
            }
            try
            {
                Snapshot.Base.IncrementDoingVersion(GrainType);//标记将要处理的Version
                var fullyEvent = new FullyEvent<PrimaryKey>
                {
                    Event = @event,
                    Base = new EventBase()
                };
                fullyEvent.StateId = GrainId;
                fullyEvent.Base.Version = Snapshot.Base.Version + 1;
                if (uniqueId == default) uniqueId = EventUID.Empty;
                if (string.IsNullOrEmpty(uniqueId.UID))
                    fullyEvent.Base.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                else
                    fullyEvent.Base.Timestamp = uniqueId.Timestamp;
                EventsInTransactionProcessing.Add(new TransactionTransport<PrimaryKey>(fullyEvent, uniqueId.UID, string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey));
                EventApply(Snapshot, fullyEvent);
                Snapshot.Base.UpdateVersion(fullyEvent.Base, GrainType);//更新处理完成的Version
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.TransactionGrainTransactionFlow, "Raise event successfully, grain Id= {0} and state version is {1}}", GrainId.ToString(), Snapshot.Base.Version);
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(LogEventIds.TransactionGrainTransactionFlow, ex, "Grain Id = {0},event type = {1} and event = {2}", GrainId.ToString(), @event.GetType().FullName, JsonSerializer.Serialize(@event));
                Snapshot.Base.DecrementDoingVersion();//还原doing Version
                throw;
            }
        }
    }
}
