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
    public abstract class TxGrain<TPrimaryKey, TStateType> : RayGrain<TPrimaryKey, TStateType>
        where TStateType : class, ICloneable<TStateType>, new()
    {
        /// <summary>
        /// Backup snapshot used for rollback during transaction
        /// </summary>
        protected Snapshot<TPrimaryKey, TStateType> BackupSnapshot { get; set; }

        /// <summary>
        /// The beginning version of the transaction
        /// </summary>
        protected long CurrentTransactionStartVersion { get; private set; } = -1;

        /// <summary>
        /// The time when the transaction started (used for timeout processing)
        /// </summary>
        protected long TransactionStartMilliseconds { get; private set; }

        /// <summary>
        /// empty: local affairs
        /// !empty: Distributed transaction
        /// </summary>
        protected string CurrentTransactionId { get; private set; }

        /// <summary>
        /// List of data to be submitted in the transaction
        /// </summary>
        protected readonly List<EventBox<TPrimaryKey>> WaitingForTransactionTransports = new List<EventBox<TPrimaryKey>>();

        /// <summary>
        /// Semaphore controller that guarantees that only one transaction is started at the same time
        /// </summary>
        private SemaphoreSlim TransactionSemaphore { get; } = new SemaphoreSlim(1, 1);

        protected override async Task RecoverySnapshot()
        {
            await base.RecoverySnapshot();
            this.BackupSnapshot = new TxSnapshot<TPrimaryKey, TStateType>(this.GrainId)
            {
                Base = this.Snapshot.Base.Clone(),
                State = this.Snapshot.State.Clone()
            };
        }

        protected override ValueTask CreateSnapshot()
        {
            this.Snapshot = new TxSnapshot<TPrimaryKey, TStateType>(this.GrainId);
            return Consts.ValueTaskDone;
        }

        protected override async Task ReadSnapshotAsync()
        {
            await base.ReadSnapshotAsync();
            this.Snapshot = new TxSnapshot<TPrimaryKey, TStateType>()
            {
                Base = new TxSnapshotBase<TPrimaryKey>(this.Snapshot.Base),
                State = this.Snapshot.State
            };
        }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            //If the committed transaction has not been Completed before inactivation, the semaphore is consumed to prevent new things from being generated
            if (this.Snapshot.Base is TxSnapshotBase<TPrimaryKey> snapshotBase)
            {
                if (snapshotBase.TransactionId != string.Empty)
                {
                    await this.TransactionSemaphore.WaitAsync();
                    var waitingEvents = await this.EventStorage.GetList(this.GrainId, snapshotBase.TransactionStartTimestamp, snapshotBase.TransactionStartVersion, this.Snapshot.Base.Version);
                    foreach (var evt in waitingEvents)
                    {
                        var transport = new EventBox<TPrimaryKey>(evt, default, string.Empty, evt.StateId.ToString());
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
        /// Restore the temporary state of the transaction
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

                            await this.RollbackTransaction(this.CurrentTransactionId);//Automatic rollback of transaction timeout
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
                        this.Snapshot = new Snapshot<TPrimaryKey, TStateType>(this.GrainId)
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
                        this.Logger.LogTrace("Transaction rolled back: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), transactionId);
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
                //If the copy snapshot is not updated, update the copy set
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
        /// Prevent objects from interfering with each other in Snapshot and BackupSnapshot, so deserialize a brand new Event object to BackupSnapshot
        /// </summary>
        /// <param name="fullyEvent">Event body</param>
        /// <param name="transport"></param>
        /// <returns></returns>
        protected override ValueTask OnRaised(FullyEvent<TPrimaryKey> fullyEvent, in EventConverter transport)
        {
            if (this.BackupSnapshot.Base.Version + 1 == fullyEvent.BasicInfo.Version)
            {
                var copiedEvent = new FullyEvent<TPrimaryKey>
                {
                    Event = this.Serializer.Deserialize(transport.EventBytes, fullyEvent.Event.GetType()) as IEvent,
                    BasicInfo = transport.BaseBytes.ParseToEventBase()
                };
                this.SnapshotHandler.Apply(this.BackupSnapshot, copiedEvent);
                this.BackupSnapshot.Base.FullUpdateVersion(copiedEvent.BasicInfo, this.GrainType);//Version of the update process
            }

            //The parent is involved in the state archive
            return base.OnRaised(fullyEvent, transport);
        }
        /// <summary>
        /// Transactional event submission
        /// The transaction must be opened before using this function, otherwise an exception will occur
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

                this.Snapshot.Base.IncrementDoingVersion(this.GrainType);//Mark the Version to be processed
                var fullyEvent = new FullyEvent<TPrimaryKey>
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

                this.WaitingForTransactionTransports.Add(new EventBox<TPrimaryKey>(fullyEvent, eUID, unique, fullyEvent.StateId.ToString()));
                this.SnapshotHandler.Apply(this.Snapshot, fullyEvent);
                this.Snapshot.Base.UpdateVersion(fullyEvent.BasicInfo, this.GrainType);//Version of the update process
                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("TxRaiseEvent completed: {0}->{1}->{2}", this.GrainType.FullName, this.Serializer.Serialize(fullyEvent), this.Serializer.Serialize(this.Snapshot));
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "TxRaiseEvent failed: {0}->{1}->{2}", this.GrainType.FullName, this.Serializer.Serialize(@event, @event.GetType()), this.Serializer.Serialize(this.Snapshot));
                this.Snapshot.Base.DecrementDoingVersion();//Restore the doing version
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
