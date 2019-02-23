using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;

namespace Ray.Core
{
    public abstract class TransactionGrain<Grain, PrimaryKey, State> : MainGrain<Grain, PrimaryKey, State>
        where State : class, ICloneable<State>, new()
    {
        public TransactionGrain(ILogger logger) : base(logger)
        {
        }
        protected Snapshot<PrimaryKey, State> BackupSnapshot { get; set; }
        protected bool TransactionPending { get; private set; }
        protected long TransactionStartVersion { get; private set; }
        protected DateTimeOffset BeginTransactionTime { get; private set; }
        private readonly List<TransactionTransport<PrimaryKey>> WaitingForTransactionEvents = new List<TransactionTransport<PrimaryKey>>();
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
                Logger.LogTrace("Begin transaction with id {0},transaction start state version {1}", GrainId.ToString(), TransactionStartVersion.ToString());
            try
            {
                if (TransactionPending)
                {
                    if ((DateTimeOffset.UtcNow - BeginTransactionTime).TotalSeconds > CoreOptions.TransactionTimeoutSeconds)
                    {
                        var rollBackTask = RollbackTransaction();//事务阻赛超过一分钟自动回滚
                        if (!rollBackTask.IsCompletedSuccessfully)
                            await rollBackTask;
                        Logger.LogError("Transaction timeout, automatic rollback,grain id = {1}", GrainId.ToString());
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
                    Logger.LogTrace("Begin transaction successfully with id {0},transaction start state version {1}", GrainId.ToString(), TransactionStartVersion.ToString());
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Begin transaction failed, grain Id = {1}", GrainId.ToString());
                throw;
            }
        }
        protected async Task CommitTransaction()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Commit transaction with id = {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), WaitingForTransactionEvents.Count.ToString(), TransactionStartVersion.ToString(), Snapshot.Base.Version.ToString());
            if (WaitingForTransactionEvents.Count > 0)
            {
                try
                {
                    foreach (var transport in WaitingForTransactionEvents)
                    {
                        var startTask = OnRaiseStart(transport.FullyEvent);
                        if (!startTask.IsCompletedSuccessfully)
                            await startTask;
                        transport.BytesTransport = new EventBytesTransport
                        {
                            EventType = transport.FullyEvent.Event.GetType().FullName,
                            ActorId = GrainId,
                            EventBytes = Serializer.SerializeToBytes(transport.FullyEvent.Event),
                            BaseBytes = transport.FullyEvent.Base.GetBytes()
                        };
                    }
                    await EventStorage.TransactionBatchAppend(WaitingForTransactionEvents);
                    foreach (var transport in WaitingForTransactionEvents)
                    {
                        var task = OnRaiseSuccessed(transport.FullyEvent, transport.BytesTransport);
                        if (!task.IsCompletedSuccessfully)
                            await task;
                    }
                    var saveSnapshotTask = SaveSnapshotAsync();
                    if (!saveSnapshotTask.IsCompletedSuccessfully)
                        await saveSnapshotTask;
                    var handlers = FollowUnit.GetAllEventHandlers();
                    if (handlers.Count > 0)
                    {
                        try
                        {
                            foreach (var transport in WaitingForTransactionEvents)
                            {
                                if (CoreOptions.PriorityAsyncEventBus)
                                {
                                    try
                                    {
                                        var publishTask = EventBusProducer.Publish(transport.BytesTransport.GetBytes(), transport.HashKey);
                                        if (!publishTask.IsCompletedSuccessfully)
                                            await publishTask;
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.LogError(ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), Snapshot.Base.Version);

                                        //当消息队列出现问题的时候同步推送
                                        await Task.WhenAll(handlers.Select(func => func(transport.BytesTransport.GetBytes())));
                                    }
                                }
                                else
                                {
                                    try
                                    {
                                        await Task.WhenAll(handlers.Select(func => func(transport.BytesTransport.GetBytes())));
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.LogError(ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), Snapshot.Base.Version);
                                        //当消息队列出现问题的时候异步推送
                                        var publishTask = EventBusProducer.Publish(transport.BytesTransport.GetBytes(), transport.HashKey);
                                        if (!publishTask.IsCompletedSuccessfully)
                                            await publishTask;
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.LogError(ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), Snapshot.Base.Version);
                        }
                    }
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace("Commit transaction with id {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), WaitingForTransactionEvents.Count.ToString(), TransactionStartVersion.ToString(), Snapshot.Base.Version.ToString());
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Commit transaction failed, grain Id = {1}", GrainId.ToString());
                    throw;
                }
                finally
                {
                    WaitingForTransactionEvents.Clear();
                    TransactionPending = false;
                }
            }
        }
        protected async ValueTask RollbackTransaction()
        {
            if (TransactionPending)
            {
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Rollback transaction successfully with id = {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), WaitingForTransactionEvents.Count.ToString(), TransactionStartVersion.ToString(), Snapshot.Base.Version.ToString());
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
                    WaitingForTransactionEvents.Clear();
                    TransactionPending = false;
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace("Rollback transaction successfully with id = {0},state version = {1}", GrainId.ToString(), Snapshot.Base.Version.ToString());
                }
                catch (Exception ex)
                {
                    Logger.LogCritical(ex, "Rollback transaction failed with Id = {1}", GrainId.ToString());
                    throw;
                }
            }
        }
        private async ValueTask TransactionStateCheck()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Check transaction with id = {0},backup version = {1},state version = {2}", GrainId.ToString(), BackupSnapshot.Base.Version, Snapshot.Base.Version);
            if (BackupSnapshot.Base.Version != Snapshot.Base.Version)
            {
                await RecoveryState();
                WaitingForTransactionEvents.Clear();
            }
        }
        protected override async Task<bool> RaiseEvent(IEvent @event, EventUID uniqueId = null)
        {
            if (TransactionPending)
            {
                var ex = new TransactionProcessingSubmitEventException(GrainId.ToString(), GrainType);
                Logger.LogError(ex, ex.Message);
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
        protected override ValueTask OnRaiseSuccessed(IFullyEvent<PrimaryKey> fullyEvent, EventBytesTransport bytesTransport)
        {
            var copiedEvent = new FullyEvent<PrimaryKey>
            {
                Event = Serializer.Deserialize(fullyEvent.Event.GetType(), bytesTransport.EventBytes) as IEvent,
                Base = EventBase.FromBytes(bytesTransport.BaseBytes)
            };
            EventHandler.Apply(BackupSnapshot, copiedEvent);
            BackupSnapshot.Base.FullUpdateVersion(copiedEvent.Base, GrainType);//更新处理完成的Version
            //父级涉及状态归档
            return base.OnRaiseSuccessed(fullyEvent, bytesTransport);
        }
        protected void TransactionRaiseEvent(IEvent @event, EventUID uniqueId = null)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start raise event by transaction, grain Id ={0} and state version = {1},event type = {2} ,event = {3},uniqueueId = {4}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, Serializer.SerializeToString(@event), uniqueId);
            if (!TransactionPending)
            {
                var ex = new UnopenTransactionException(GrainId.ToString(), GrainType, nameof(TransactionRaiseEvent));
                Logger.LogError(ex, ex.Message);
                throw ex;
            }
            try
            {
                Snapshot.Base.IncrementDoingVersion(GrainType);//标记将要处理的Version
                var fullyEvent = new FullyEvent<PrimaryKey>
                {
                    StateId = GrainId,
                    Event = @event,
                    Base = new EventBase
                    {
                        Version = Snapshot.Base.Version + 1
                    }
                };
                if (uniqueId == default) uniqueId = EventUID.Empty;
                if (string.IsNullOrEmpty(uniqueId.UID))
                    fullyEvent.Base.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                else
                    fullyEvent.Base.Timestamp = uniqueId.Timestamp;
                WaitingForTransactionEvents.Add(new TransactionTransport<PrimaryKey>(fullyEvent, uniqueId.UID, fullyEvent.StateId.ToString()));
                EventHandler.Apply(Snapshot, fullyEvent);
                Snapshot.Base.UpdateVersion(fullyEvent.Base, GrainType);//更新处理完成的Version
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Raise event successfully, grain Id= {0} and state version is {1}}", GrainId.ToString(), Snapshot.Base.Version);
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Grain Id = {0},event type = {1} and event = {2}", GrainId.ToString(), @event.GetType().FullName, Serializer.SerializeToString(@event));
                Snapshot.Base.DecrementDoingVersion();//还原doing Version
                throw;
            }
        }
    }
}
