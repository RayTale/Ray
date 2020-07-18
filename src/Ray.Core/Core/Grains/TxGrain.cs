using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core.Abstractions.Monitor;
using Ray.Core.Event;
using Ray.Core.Exceptions;
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
        /// empty:本地事务
        /// !empty:分布式事务
        /// </summary>
        protected string CurrentTransactionId { get; private set; }

        /// <summary>
        /// 事务中待提交的数据列表
        /// </summary>
        protected readonly List<EventBox<PrimaryKey>> WaitingForTransactionTransports = new List<EventBox<PrimaryKey>>();

        /// <summary>
        /// 保证同一时间只有一个事务启动的信号量控制器
        /// </summary>
        private SemaphoreSlim TransactionSemaphore { get; } = new SemaphoreSlim(1, 1);

        protected override async Task RecoverySnapshot()
        {
            await base.RecoverySnapshot();
            this.BackupSnapshot = new TxSnapshot<PrimaryKey, StateType>(this.GrainId)
            {
                Base = this.Snapshot.Base.Clone(),
                State = this.Snapshot.State.Clone()
            };
        }

        protected override ValueTask CreateSnapshot()
        {
            this.Snapshot = new TxSnapshot<PrimaryKey, StateType>(this.GrainId);
            return Consts.ValueTaskDone;
        }

        protected override async Task ReadSnapshotAsync()
        {
            await base.ReadSnapshotAsync();
            this.Snapshot = new TxSnapshot<PrimaryKey, StateType>()
            {
                Base = new TxSnapshotBase<PrimaryKey>(this.Snapshot.Base),
                State = this.Snapshot.State
            };
        }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            //如果失活之前已提交事务还没有Complete,则消耗信号量，防止产生新的事物
            if (this.Snapshot.Base is TxSnapshotBase<PrimaryKey> snapshotBase)
            {
                if (snapshotBase.TransactionId != string.Empty)
                {
                    await this.TransactionSemaphore.WaitAsync();
                    var waitingEvents = await this.EventStorage.GetList(this.GrainId, snapshotBase.TransactionStartTimestamp, snapshotBase.TransactionStartVersion, this.Snapshot.Base.Version);
                    foreach (var evt in waitingEvents)
                    {
                        var transport = new EventBox<PrimaryKey>(evt, default, string.Empty, evt.StateId.ToString());
                        transport.Parse(this.TypeFinder, this.Serializer);
                        this.WaitingForTransactionTransports.Add(transport);
                    }

                    this.CurrentTransactionId = snapshotBase.TransactionId;
                    this.CurrentTransactionStartVersion = snapshotBase.TransactionStartVersion;
                }
            }
            else
            {
                throw new SnapshotNotSupportTxException(this.Snapshot.GetType());
            }
        }

        /// <summary>
        /// 复原事务临时状态
        /// </summary>
        private void RestoreTransactionTemporaryState()
        {
            this.CurrentTransactionId = string.Empty;
            this.CurrentTransactionStartVersion = -1;
            this.TransactionStartMilliseconds = 0;
        }

        private SemaphoreSlim TransactionTimeoutLock { get; } = new SemaphoreSlim(1, 1);

        protected async Task BeginTransaction(string transactionId)
        {
            if (this.Logger.IsEnabled(LogLevel.Trace))
            {
                this.Logger.LogTrace("Transaction begin: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), transactionId);
            }

            if (this.TransactionStartMilliseconds != 0 &&
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - this.TransactionStartMilliseconds > this.CoreOptions.TransactionTimeout)
            {
                if (await this.TransactionTimeoutLock.WaitAsync(this.CoreOptions.TransactionTimeout))
                {
                    try
                    {
                        if (this.TransactionStartMilliseconds != 0 &&
                            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - this.TransactionStartMilliseconds > this.CoreOptions.TransactionTimeout)
                        {
                            if (this.Logger.IsEnabled(LogLevel.Trace))
                            {
                                this.Logger.LogTrace("Transaction timeout: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), transactionId);
                            }

                            await this.RollbackTransaction(this.CurrentTransactionId);//事务超时自动回滚
                        }
                    }
                    finally
                    {
                        this.TransactionTimeoutLock.Release();
                    }
                }
            }

            if (await this.TransactionSemaphore.WaitAsync(this.CoreOptions.TransactionTimeout))
            {
                try
                {
                    this.SnapshotCheck();
                    this.CurrentTransactionStartVersion = this.Snapshot.Base.Version + 1;
                    this.CurrentTransactionId = transactionId;
                    this.TransactionStartMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                }
                catch
                {
                    this.TransactionSemaphore.Release();
                    throw;
                }
            }
            else
            {
                throw new BeginTxTimeoutException(this.GrainId.ToString(), transactionId, this.GrainType);
            }

        }

        public async Task CommitTransaction(string transactionId)
        {
            if (this.WaitingForTransactionTransports.Count > 0)
            {
                if (this.CurrentTransactionId != transactionId)
                {
                    throw new TxCommitException();
                }

                try
                {
                    var onCommitTask = this.OnCommitTransaction(transactionId);
                    if (!onCommitTask.IsCompletedSuccessfully)
                    {
                        await onCommitTask;
                    }

                    foreach (var transport in this.WaitingForTransactionTransports)
                    {
                        var startTask = this.OnRaiseStart(transport.FullyEvent);
                        if (!startTask.IsCompletedSuccessfully)
                        {
                            await startTask;
                        }

                        transport.Parse(this.TypeFinder, this.Serializer);
                    }

                    if (this.MetricMonitor != default)
                    {
                        var startTime = DateTimeOffset.UtcNow;
                        await this.EventStorage.TransactionBatchAppend(this.WaitingForTransactionTransports);
                        var nowTime = DateTimeOffset.UtcNow;
                        var metricList = this.WaitingForTransactionTransports.Select(evt => new EventMetricElement
                        {
                            Actor = this.GrainType.Name,
                            ActorId = this.GrainId.ToString(),
                            Event = evt.FullyEvent.Event.GetType().Name,
                            FromEvent = evt.EventUID?.FromEvent,
                            FromEventActor = evt.EventUID?.FromActor,
                            InsertElapsedMs = (int)nowTime.Subtract(startTime).TotalMilliseconds,
                            IntervalPrevious = evt.EventUID == default ? 0 : (int)(nowTime.ToUnixTimeMilliseconds() - evt.EventUID.Timestamp),
                            Ignore = false,
                        }).ToList();
                        this.MetricMonitor.Report(metricList);
                    }
                    else
                    {
                        await this.EventStorage.TransactionBatchAppend(this.WaitingForTransactionTransports);
                    }

                    if (this.Logger.IsEnabled(LogLevel.Trace))
                    {
                        this.Logger.LogTrace("Transaction Commited: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), transactionId);
                    }
                }
                catch (Exception ex)
                {
                    this.Logger.LogCritical(ex, "Transaction failed: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), transactionId);
                    throw;
                }
            }
        }

        protected virtual ValueTask OnCommitTransaction(string transactionId) => Consts.ValueTaskDone;

        public async Task RollbackTransaction(string transactionId)
        {
            if (this.CurrentTransactionId == transactionId && this.CurrentTransactionStartVersion != -1 && this.Snapshot.Base.Version >= this.CurrentTransactionStartVersion)
            {
                try
                {
                    if (this.BackupSnapshot.Base.Version == this.CurrentTransactionStartVersion - 1)
                    {
                        this.Snapshot = new Snapshot<PrimaryKey, StateType>(this.GrainId)
                        {
                            Base = this.BackupSnapshot.Base.Clone(),
                            State = this.BackupSnapshot.State.Clone()
                        };
                    }
                    else
                    {
                        if (this.BackupSnapshot.Base.Version >= this.CurrentTransactionStartVersion)
                        {
                            await this.EventStorage.DeleteAfter(this.Snapshot.Base.StateId, this.CurrentTransactionStartVersion, this.Snapshot.Base.LatestMinEventTimestamp);
                        }

                        await this.RecoverySnapshot();
                    }

                    this.WaitingForTransactionTransports.Clear();
                    this.RestoreTransactionTemporaryState();
                    this.TransactionSemaphore.Release();
                    if (this.Logger.IsEnabled(LogLevel.Trace))
                    {
                        this.Logger.LogTrace("Transaction rollbacked: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), transactionId);
                    }
                }
                catch (Exception ex)
                {
                    this.Logger.LogCritical(ex, "Transaction rollback failed: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), transactionId);
                    throw;
                }
            }
        }

        public async Task FinishTransaction(string transactionId)
        {
            if (this.CurrentTransactionId == transactionId)
            {
                //如果副本快照没有更新，则更新副本集
                foreach (var transport in this.WaitingForTransactionTransports)
                {
                    var task = this.OnRaised(transport.FullyEvent, transport.GetConverter());
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }
                }

                var onFinishTask = this.OnFinshTransaction(transactionId);
                if (!onFinishTask.IsCompletedSuccessfully)
                {
                    await onFinishTask;
                }

                var saveSnapshotTask = this.SaveSnapshotAsync();
                if (!saveSnapshotTask.IsCompletedSuccessfully)
                {
                    await saveSnapshotTask;
                }

                var handlers = this.ObserverUnit.GetAllEventHandlers();
                if (handlers.Count > 0)
                {
                    try
                    {
                        foreach (var transport in this.WaitingForTransactionTransports)
                        {
                            await this.PublishToEventBus(transport.GetSpan().ToArray(), transport.HashKey);
                        }
                    }
                    catch (Exception ex)
                    {
                        this.Logger.LogError(ex, ex.Message);
                    }
                }

                this.WaitingForTransactionTransports.ForEach(transport => transport.Dispose());
                this.WaitingForTransactionTransports.Clear();
                this.RestoreTransactionTemporaryState();
                this.TransactionSemaphore.Release();
            }
        }

        protected virtual ValueTask OnFinshTransaction(string transactionId) => Consts.ValueTaskDone;

        private void SnapshotCheck()
        {
            if (this.BackupSnapshot.Base.Version != this.Snapshot.Base.Version)
            {
                var ex = new TxSnapshotException(this.Snapshot.Base.StateId.ToString(), this.Snapshot.Base.Version, this.BackupSnapshot.Base.Version);
                this.Logger.LogCritical(ex, nameof(this.SnapshotCheck));
                throw ex;
            }
        }

        protected override async Task<bool> RaiseEvent(IEvent @event, EventUID uniqueId = null)
        {
            if (await this.TransactionSemaphore.WaitAsync(this.CoreOptions.TransactionTimeout))
            {
                try
                {
                    this.SnapshotCheck();
                    return await base.RaiseEvent(@event, uniqueId);
                }
                finally
                {
                    this.TransactionSemaphore.Release();
                }
            }
            else
            {
                throw new BeginTxTimeoutException(this.GrainId.ToString(), string.Empty, this.GrainType);
            }
        }

        /// <summary>
        /// 防止对象在Snapshot和BackupSnapshot中互相干扰，所以反序列化一个全新的Event对象给BackupSnapshot
        /// </summary>
        /// <param name="fullyEvent">事件本体</param>
        /// <param name="bytes">事件序列化之后的二进制数据</param>
        /// <returns></returns>
        protected override ValueTask OnRaised(FullyEvent<PrimaryKey> fullyEvent, in EventConverter transport)
        {
            if (this.BackupSnapshot.Base.Version + 1 == fullyEvent.BasicInfo.Version)
            {
                var copiedEvent = new FullyEvent<PrimaryKey>
                {
                    Event = this.Serializer.Deserialize(transport.EventBytes, fullyEvent.Event.GetType()) as IEvent,
                    BasicInfo = transport.BaseBytes.ParseToEventBase()
                };
                this.SnapshotHandler.Apply(this.BackupSnapshot, copiedEvent);
                this.BackupSnapshot.Base.FullUpdateVersion(copiedEvent.BasicInfo, this.GrainType);//更新处理完成的Version
            }

            //父级涉及状态归档
            return base.OnRaised(fullyEvent, transport);
        }

        /// <summary>
        /// 事务性事件提交
        /// 使用该函数前必须开启事务，不然会出现异常
        /// </summary>
        /// <param name="event"></param>
        /// <param name="eUID"></param>
        protected void TxRaiseEvent(IEvent @event, EventUID eUID = null)
        {
            try
            {
                if (this.CurrentTransactionStartVersion == -1)
                {
                    throw new UnopenedTransactionException(this.GrainId.ToString(), this.GrainType, nameof(this.TxRaiseEvent));
                }

                this.Snapshot.Base.IncrementDoingVersion(this.GrainType);//标记将要处理的Version
                var fullyEvent = new FullyEvent<PrimaryKey>
                {
                    StateId = this.GrainId,
                    Event = @event,
                    BasicInfo = new EventBasicInfo
                    {
                        Version = this.Snapshot.Base.Version + 1
                    }
                };
                string unique = default;
                if (eUID is null)
                {
                    fullyEvent.BasicInfo.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    unique = fullyEvent.GetEventId();
                }
                else
                {
                    fullyEvent.BasicInfo.Timestamp = eUID.Timestamp;
                    unique = eUID.UID;
                }

                this.WaitingForTransactionTransports.Add(new EventBox<PrimaryKey>(fullyEvent, eUID, unique, fullyEvent.StateId.ToString()));
                this.SnapshotHandler.Apply(this.Snapshot, fullyEvent);
                this.Snapshot.Base.UpdateVersion(fullyEvent.BasicInfo, this.GrainType);//更新处理完成的Version
                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("TxRaiseEvent completed: {0}->{1}->{2}", this.GrainType.FullName, this.Serializer.Serialize(fullyEvent), this.Serializer.Serialize(this.Snapshot));
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "TxRaiseEvent failed: {0}->{1}->{2}", this.GrainType.FullName, this.Serializer.Serialize(@event, @event.GetType()), this.Serializer.Serialize(this.Snapshot));
                this.Snapshot.Base.DecrementDoingVersion();//还原doing Version
                throw;
            }
        }

        protected async Task TxRaiseEvent(string transactionId, IEvent @event, EventUID uniqueId = null)
        {
            if (transactionId == default)
            {
                throw new TxIdException();
            }

            if (transactionId != this.CurrentTransactionId)
            {
                await this.BeginTransaction(transactionId);
            }

            this.TxRaiseEvent(@event, uniqueId);
        }
    }
}
