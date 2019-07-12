using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;

namespace Ray.Core
{
    public abstract class TxGrain<PrimaryKey, StateType> : RayGrain<PrimaryKey, StateType>
        where StateType : class, ICloneable<StateType>, new()
    {
        /// <summary>
        /// 事务过程中用于回滚的备份快照
        /// </summary>
        protected Snapshot<PrimaryKey, StateType> BackupSnapshot { get; set; }
        /// <summary>
        /// 事务的开始版本
        /// </summary>
        protected long CurrentTransactionStartVersion { get; private set; } = -1;
        /// <summary>
        /// 事务开始的时间(用作超时处理)
        /// </summary>
        protected long TransactionStartMilliseconds { get; private set; }
        /// <summary>
        /// 0:本地事务
        /// >0:分布式事务
        /// </summary>
        protected long CurrentTransactionId { get; private set; }
        /// <summary>
        /// 事务中待提交的数据列表
        /// </summary>
        protected readonly List<EventTransport<PrimaryKey>> WaitingForTransactionTransports = new List<EventTransport<PrimaryKey>>();
        /// <summary>
        /// 保证同一时间只有一个事务启动的信号量控制器
        /// </summary>
        private SemaphoreSlim TransactionSemaphore { get; } = new SemaphoreSlim(1, 1);
        protected override async Task RecoverySnapshot()
        {
            await base.RecoverySnapshot();
            BackupSnapshot = new TxSnapshot<PrimaryKey, StateType>(GrainId)
            {
                Base = Snapshot.Base.Clone(),
                State = Snapshot.State.Clone()
            };
        }
        protected override ValueTask CreateSnapshot()
        {
            Snapshot = new TxSnapshot<PrimaryKey, StateType>(GrainId);
            return Consts.ValueTaskDone;
        }
        protected override async Task ReadSnapshotAsync()
        {
            await base.ReadSnapshotAsync();
            Snapshot = new TxSnapshot<PrimaryKey, StateType>()
            {
                Base = new TxSnapshotBase<PrimaryKey>(Snapshot.Base),
                State = Snapshot.State
            };
        }
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            //如果失活之前已提交事务还没有Complete,则消耗信号量，防止产生新的事物
            if (Snapshot.Base is TxSnapshotBase<PrimaryKey> snapshotBase)
            {
                if (snapshotBase.TransactionId != 0)
                {
                    await TransactionSemaphore.WaitAsync();
                    var waitingEvents = await EventStorage.GetList(GrainId, snapshotBase.TransactionStartTimestamp, snapshotBase.TransactionStartVersion, Snapshot.Base.Version);
                    foreach (var evt in waitingEvents)
                    {
                        WaitingForTransactionTransports.Add(new EventTransport<PrimaryKey>(evt, string.Empty, evt.StateId.ToString())
                        {
                            BytesTransport = new EventBytesTransport(
                                TypeContainer.GetTypeCode(evt.Event.GetType()),
                                GrainId,
                                evt.Base.GetBytes(),
                                Serializer.SerializeToBytes(evt.Event)
                            )
                        });
                    }
                    CurrentTransactionId = snapshotBase.TransactionId;
                    CurrentTransactionStartVersion = snapshotBase.TransactionStartVersion;
                }
            }
            else
            {
                throw new SnapshotNotSupportTxException(Snapshot.GetType());
            }
        }
        /// <summary>
        /// 复原事务临时状态
        /// </summary>
        private void RestoreTransactionTemporaryState()
        {
            CurrentTransactionId = 0;
            CurrentTransactionStartVersion = -1;
            TransactionStartMilliseconds = 0;
        }
        private SemaphoreSlim _transactionTimeoutLock { get; } = new SemaphoreSlim(1, 1);
        protected async Task BeginTransaction(long transactionId)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Begin transaction with grainid {0} and transactionId {1},transaction start state version {2}", GrainId.ToString(), transactionId, CurrentTransactionStartVersion.ToString());
            if (TransactionStartMilliseconds != 0 &&
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TransactionStartMilliseconds > CoreOptions.TransactionMillisecondsTimeout)
            {
                if (await _transactionTimeoutLock.WaitAsync(CoreOptions.TransactionMillisecondsTimeout))
                {
                    try
                    {
                        if (TransactionStartMilliseconds != 0 &&
                            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TransactionStartMilliseconds > CoreOptions.TransactionMillisecondsTimeout)
                        {
                            await RollbackTransaction(CurrentTransactionId);//事务超时自动回滚
                            Logger.LogError("Transaction timeout, automatic rollback,grain id = {1}", GrainId.ToString());
                        }
                    }
                    finally
                    {
                        _transactionTimeoutLock.Release();
                    }
                }
            }
            if (await TransactionSemaphore.WaitAsync(CoreOptions.TransactionMillisecondsTimeout))
            {
                try
                {
                    SnapshotCheck();
                    CurrentTransactionStartVersion = Snapshot.Base.Version + 1;
                    CurrentTransactionId = transactionId;
                    TransactionStartMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                }
                catch
                {
                    TransactionSemaphore.Release();
                    throw;
                }
            }
            else
            {
                throw new BeginTxTimeoutException(GrainId.ToString(), transactionId, GrainType);
            }

        }
        public async Task CommitTransaction(long transactionId)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Commit transaction with id = {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), WaitingForTransactionTransports.Count.ToString(), CurrentTransactionStartVersion.ToString(), Snapshot.Base.Version.ToString());
            if (WaitingForTransactionTransports.Count > 0)
            {
                if (CurrentTransactionId != transactionId)
                    throw new TxCommitException();
                try
                {
                    var onCommitTask = OnCommitTransaction(transactionId);
                    if (!onCommitTask.IsCompletedSuccessfully)
                        await onCommitTask;
                    foreach (var transport in WaitingForTransactionTransports)
                    {
                        var startTask = OnRaiseStart(transport.FullyEvent);
                        if (!startTask.IsCompletedSuccessfully)
                            await startTask;
                        transport.BytesTransport = new EventBytesTransport(
                            TypeContainer.GetTypeCode(transport.FullyEvent.Event.GetType()),
                            GrainId,
                            transport.FullyEvent.Base.GetBytes(),
                            Serializer.SerializeToBytes(transport.FullyEvent.Event)
                        );
                    }
                    await EventStorage.TransactionBatchAppend(WaitingForTransactionTransports);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Commit transaction failed, grain Id = {1}", GrainId.ToString());
                    throw;
                }
            }
        }
        protected virtual ValueTask OnCommitTransaction(long transactionId) => Consts.ValueTaskDone;
        public async Task RollbackTransaction(long transactionId)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start rollback transaction with id = {0},event counts = {1}, from version {2} to version {3}", GrainId.ToString(), WaitingForTransactionTransports.Count.ToString(), CurrentTransactionStartVersion.ToString(), Snapshot.Base.Version.ToString());
            if (CurrentTransactionId == transactionId && CurrentTransactionStartVersion != -1 && Snapshot.Base.Version >= CurrentTransactionStartVersion)
            {
                try
                {
                    if (BackupSnapshot.Base.Version == CurrentTransactionStartVersion - 1)
                    {
                        Snapshot = new Snapshot<PrimaryKey, StateType>(GrainId)
                        {
                            Base = BackupSnapshot.Base.Clone(),
                            State = BackupSnapshot.State.Clone()
                        };
                    }
                    else
                    {
                        if (BackupSnapshot.Base.Version >= CurrentTransactionStartVersion)
                            await EventStorage.DeleteEnd(Snapshot.Base.StateId, CurrentTransactionStartVersion, Snapshot.Base.LatestMinEventTimestamp);
                        await RecoverySnapshot();
                    }

                    WaitingForTransactionTransports.Clear();
                    RestoreTransactionTemporaryState();
                    TransactionSemaphore.Release();
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
        public async Task FinishTransaction(long transactionId)
        {
            if (CurrentTransactionId == transactionId)
            {
                //如果副本快照没有更新，则更新副本集
                foreach (var transport in WaitingForTransactionTransports)
                {
                    var task = OnRaised(transport.FullyEvent, transport.BytesTransport);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                }
                var onFinishTask = OnFinshTransaction(transactionId);
                if (!onFinishTask.IsCompletedSuccessfully)
                    await onFinishTask;
                var saveSnapshotTask = SaveSnapshotAsync();
                if (!saveSnapshotTask.IsCompletedSuccessfully)
                    await saveSnapshotTask;
                var handlers = ObserverUnit.GetAllEventHandlers();
                if (handlers.Count > 0)
                {
                    try
                    {
                        foreach (var transport in WaitingForTransactionTransports)
                        {
                            await PublishToEventBust(transport.BytesTransport.GetBytes(), transport.HashKey);
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex, ex.Message);
                    }
                }
                WaitingForTransactionTransports.Clear();
                RestoreTransactionTemporaryState();
                TransactionSemaphore.Release();
            }
        }
        protected virtual ValueTask OnFinshTransaction(long transactionId) => Consts.ValueTaskDone;
        private void SnapshotCheck()
        {
            if (BackupSnapshot.Base.Version != Snapshot.Base.Version)
            {
                var ex = new TxSnapshotException(Snapshot.Base.StateId.ToString(), Snapshot.Base.Version, BackupSnapshot.Base.Version);
                Logger.LogCritical(ex, nameof(SnapshotCheck));
                throw ex;
            }
        }
        protected override async Task<bool> RaiseEvent(IEvent @event, EventUID uniqueId = null)
        {
            if (await TransactionSemaphore.WaitAsync(CoreOptions.TransactionMillisecondsTimeout))
            {
                try
                {
                    SnapshotCheck();
                    return await base.RaiseEvent(@event, uniqueId);
                }
                finally
                {
                    TransactionSemaphore.Release();
                }
            }
            else
            {
                throw new BeginTxTimeoutException(GrainId.ToString(), -1, GrainType);
            }
        }
        /// <summary>
        /// 防止对象在Snapshot和BackupSnapshot中互相干扰，所以反序列化一个全新的Event对象给BackupSnapshot
        /// </summary>
        /// <param name="fullyEvent">事件本体</param>
        /// <param name="bytes">事件序列化之后的二进制数据</param>
        protected override ValueTask OnRaised(IFullyEvent<PrimaryKey> fullyEvent, EventBytesTransport transport)
        {
            if (BackupSnapshot.Base.Version + 1 == fullyEvent.Base.Version)
            {
                var copiedEvent = new FullyEvent<PrimaryKey>
                {
                    Event = Serializer.Deserialize(fullyEvent.Event.GetType(), transport.EventBytes) as IEvent,
                    Base = EventBase.FromBytes(transport.BaseBytes)
                };
                SnapshotHandler.Apply(BackupSnapshot, copiedEvent);
                BackupSnapshot.Base.FullUpdateVersion(copiedEvent.Base, GrainType);//更新处理完成的Version
            }
            //父级涉及状态归档
            return base.OnRaised(fullyEvent, transport);
        }
        /// <summary>
        /// 事务性事件提交
        /// 使用该函数前必须开启事务，不然会出现异常
        /// </summary>
        /// <param name="event"></param>
        /// <param name="uniqueId"></param>
        protected void TxRaiseEvent(IEvent @event, EventUID uniqueId = null)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start transactionRaiseEvent, grain Id ={0} and state version = {1},event type = {2} ,event = {3},uniqueueId = {4}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, Serializer.SerializeToString(@event), uniqueId);
            try
            {
                if (CurrentTransactionStartVersion == -1)
                {
                    throw new UnopenedTransactionException(GrainId.ToString(), GrainType, nameof(TxRaiseEvent));
                }
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
                WaitingForTransactionTransports.Add(new EventTransport<PrimaryKey>(fullyEvent, uniqueId.UID, fullyEvent.StateId.ToString()));
                SnapshotHandler.Apply(Snapshot, fullyEvent);
                Snapshot.Base.UpdateVersion(fullyEvent.Base, GrainType);//更新处理完成的Version
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Grain Id = {0},event type = {1} and event = {2}", GrainId.ToString(), @event.GetType().FullName, Serializer.SerializeToString(@event));
                Snapshot.Base.DecrementDoingVersion();//还原doing Version
                throw;
            }
        }
        protected async Task TxRaiseEvent(long transactionId, IEvent @event, EventUID uniqueId = null)
        {
            if (transactionId <= 0)
                throw new TxIdException();
            if (transactionId != CurrentTransactionId)
            {
                await BeginTransaction(transactionId);
            }
            TxRaiseEvent(@event, uniqueId);
        }
    }
}
