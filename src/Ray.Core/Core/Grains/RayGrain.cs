using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Abstractions;
using Ray.Core.Abstractions.Monitor;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Services;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Core
{
    public abstract class RayGrain<PrimaryKey, StateType> : Grain, IObservable
        where StateType : class, new()
    {
        public RayGrain()
        {
            this.GrainType = this.GetType();
        }

        protected CoreOptions CoreOptions { get; private set; }

        protected ArchiveOptions ArchiveOptions { get; private set; }

        protected ILogger Logger { get; private set; }

        protected IProducerContainer ProducerContainer { get; private set; }

        protected ISerializer Serializer { get; private set; }

        protected ITypeFinder TypeFinder { get; private set; }

        protected Snapshot<PrimaryKey, StateType> Snapshot { get; set; }

        protected ISnapshotHandler<PrimaryKey, StateType> SnapshotHandler { get; private set; }

        protected IObserverUnit<PrimaryKey> ObserverUnit { get; private set; }

        /// <summary>
        /// 归档存储器
        /// </summary>
        protected IArchiveStorage<PrimaryKey, StateType> ArchiveStorage { get; private set; }

        protected List<ArchiveBrief> BriefArchiveList { get; private set; }

        protected ArchiveBrief LastArchive { get; private set; }

        protected ArchiveBrief NewArchive { get; private set; }

        protected ArchiveBrief ClearedArchive { get; private set; }

        private PrimaryKey GrainId;
        private bool GrainIdAcquired = false;

        public PrimaryKey GrainId
        {
            get
            {
                if (!this.GrainIdAcquired)
                {
                    var type = typeof(PrimaryKey);
                    if (type == typeof(long) && this.GetPrimaryKeyLong() is PrimaryKey longKey)
                    {
                        this.GrainId = longKey;
                    }
                    else if (type == typeof(string) && this.GetPrimaryKeyString() is PrimaryKey stringKey)
                    {
                        this.GrainId = stringKey;
                    }
                    else if (type == typeof(Guid) && this.GetPrimaryKey() is PrimaryKey guidKey)
                    {
                        this.GrainId = guidKey;
                    }
                    else
                    {
                        throw new ArgumentOutOfRangeException(typeof(PrimaryKey).FullName);
                    }

                    this.GrainIdAcquired = true;
                }

                return this.GrainId;
            }
        }

        /// <summary>
        /// 快照的事件版本号
        /// </summary>
        protected long SnapshotEventVersion { get; private set; }

        /// <summary>
        /// 当前Grain的真实Type
        /// </summary>
        protected Type GrainType { get; }

        /// <summary>
        /// 事件存储器
        /// </summary>
        protected IEventStorage<PrimaryKey> EventStorage { get; private set; }

        /// <summary>
        /// 状态存储器
        /// </summary>
        protected ISnapshotStorage<PrimaryKey, StateType> SnapshotStorage { get; private set; }

        /// <summary>
        /// 事件发布器
        /// </summary>
        protected IProducer EventBusProducer { get; private set; }

        /// <summary>
        /// 指标收集器
        /// </summary>
        protected IMetricMonitor MetricMonitor { get; private set; }

        /// <summary>
        /// 事件处理器
        /// </summary>
        protected List<Func<BytesBox, Task>> ObserverEventHandlers { get; private set; }

        /// <summary>
        /// 依赖注入统一方法
        /// </summary>
        /// <returns></returns>
        protected async virtual ValueTask DependencyInjection()
        {
            this.CoreOptions = this.ServiceProvider.GetOptionsByName<CoreOptions>(this.GrainType.FullName);
            this.ArchiveOptions = this.ServiceProvider.GetOptionsByName<ArchiveOptions>(this.GrainType.FullName);
            this.Logger = (ILogger)this.ServiceProvider.GetService(typeof(ILogger<>).MakeGenericType(this.GrainType));
            this.MetricMonitor = this.ServiceProvider.GetService<IMetricMonitor>();
            this.ProducerContainer = this.ServiceProvider.GetService<IProducerContainer>();
            this.Serializer = this.ServiceProvider.GetService<ISerializer>();
            this.TypeFinder = this.ServiceProvider.GetService<ITypeFinder>();
            this.SnapshotHandler = this.ServiceProvider.GetService<ISnapshotHandler<PrimaryKey, StateType>>();
            if (this.SnapshotHandler == default)
            {
                throw new UnfindSnapshotHandlerException(this.GrainType);
            }

            this.ObserverUnit = this.ServiceProvider.GetService<IObserverUnitContainer>().GetUnit<PrimaryKey>(this.GrainType);
            this.ObserverEventHandlers = this.ObserverUnit.GetAllEventHandlers();
            var configureBuilder = (IConfigureBuilder<PrimaryKey>)this.ServiceProvider.GetService(typeof(IConfigureBuilder<,>).MakeGenericType(typeof(PrimaryKey), this.GrainType));
            var storageConfigTask = configureBuilder.GetConfig(this.ServiceProvider, this.GrainId);
            if (!storageConfigTask.IsCompletedSuccessfully)
            {
                await storageConfigTask;
            }

            var storageFactory = this.ServiceProvider.GetService(configureBuilder.StorageFactory) as IStorageFactory;
            //创建归档存储器
            if (this.ArchiveOptions.On)
            {
                var archiveStorageTask = storageFactory.CreateArchiveStorage<PrimaryKey, StateType>(storageConfigTask.Result, this.GrainId);
                if (!archiveStorageTask.IsCompletedSuccessfully)
                {
                    await archiveStorageTask;
                }

                this.ArchiveStorage = archiveStorageTask.Result;
            }

            //创建事件存储器
            var eventStorageTask = storageFactory.CreateEventStorage(storageConfigTask.Result, this.GrainId);
            if (!eventStorageTask.IsCompletedSuccessfully)
            {
                await eventStorageTask;
            }

            this.EventStorage = eventStorageTask.Result;
            //创建状态存储器
            var stateStorageTask = storageFactory.CreateSnapshotStorage<PrimaryKey, StateType>(storageConfigTask.Result, this.GrainId);
            if (!stateStorageTask.IsCompletedSuccessfully)
            {
                await stateStorageTask;
            }

            this.SnapshotStorage = stateStorageTask.Result;
            //创建事件发布器
            var producerTask = this.ProducerContainer.GetProducer(this.GrainType);
            if (!producerTask.IsCompletedSuccessfully)
            {
                await producerTask;
            }

            this.EventBusProducer = producerTask.Result;
        }

        /// <summary>
        /// Grain激活时调用用来初始化的方法(禁止在子类重写)
        /// </summary>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        public override async Task OnActivateAsync()
        {
            var dITask = this.DependencyInjection();
            if (!dITask.IsCompletedSuccessfully)
            {
                await dITask;
            }

            try
            {
                if (this.ArchiveOptions.On)
                {
                    //加载归档信息
                    this.BriefArchiveList = (await this.ArchiveStorage.GetBriefList(this.GrainId)).OrderBy(a => a.Index).ToList();
                    this.LastArchive = this.BriefArchiveList.LastOrDefault();
                    this.ClearedArchive = this.BriefArchiveList.Where(a => a.EventIsCleared).OrderByDescending(a => a.Index).FirstOrDefault();
                    var secondLastArchive = this.BriefArchiveList.Count > 1 ? this.BriefArchiveList.SkipLast(1).Last() : default;
                    if (this.LastArchive != null && !this.LastArchive.IsCompletedArchive(this.ArchiveOptions, secondLastArchive) && !this.LastArchive.EventIsCleared)
                    {
                        await this.DeleteArchive(this.LastArchive.Id);
                        this.BriefArchiveList.Remove(this.LastArchive);
                        this.NewArchive = this.LastArchive;
                        this.LastArchive = this.BriefArchiveList.LastOrDefault();
                    }
                }

                //修复状态
                await this.RecoverySnapshot();

                if (this.ArchiveOptions.On)
                {
                    if (this.Snapshot.Base.Version != 0 &&
                        (this.LastArchive is null || this.LastArchive.EndVersion < this.Snapshot.Base.Version) &&
                        (this.NewArchive is null || this.NewArchive.EndVersion < this.Snapshot.Base.Version))
                    {
                        //归档恢复
                        while (true)
                        {
                            var startTimestamp = this.Snapshot.Base.StartTimestamp;
                            long startVersion = 0;
                            if (this.NewArchive != null)
                            {
                                startVersion = this.NewArchive.EndVersion;
                                startTimestamp = this.NewArchive.StartTimestamp;
                            }
                            else if (this.NewArchive is null && this.LastArchive != null)
                            {
                                startVersion = this.LastArchive.EndVersion;
                                startTimestamp = this.LastArchive.EndTimestamp;
                            }

                            var eventList = await this.EventStorage.GetList(this.GrainId, startTimestamp, startVersion + 1, startVersion + this.CoreOptions.NumberOfEventsPerRead);
                            foreach (var @event in eventList)
                            {
                                var task = this.EventArchive(@event);
                                if (!task.IsCompletedSuccessfully)
                                {
                                    await task;
                                }
                            }

                            if (eventList.Count < this.CoreOptions.NumberOfEventsPerRead)
                            {
                                break;
                            }
                        }

                    }
                }

                if (this.CoreOptions.SyncAllObserversOnActivate)
                {
                    var syncResults = await this.ObserverUnit.SyncAllObservers(this.GrainId, this.Snapshot.Base.Version);
                    if (syncResults.Any(r => !r))
                    {
                        throw new SyncAllObserversException(this.GrainId.ToString(), this.GrainType);
                    }
                }

                var onActivatedTask = this.OnActivationCompleted();
                if (!onActivatedTask.IsCompletedSuccessfully)
                {
                    await onActivatedTask;
                }

                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("Activation completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "Activation failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                throw;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnActivationCompleted() => Consts.ValueTaskDone;

        protected virtual async Task RecoverySnapshot()
        {
            try
            {
                await this.ReadSnapshotAsync();
                while (!this.Snapshot.Base.IsLatest)
                {
                    var eventList = await this.EventStorage.GetList(this.GrainId, this.Snapshot.Base.LatestMinEventTimestamp, this.Snapshot.Base.Version + 1, this.Snapshot.Base.Version + this.CoreOptions.NumberOfEventsPerRead);
                    foreach (var fullyEvent in eventList)
                    {
                        this.Snapshot.Base.IncrementDoingVersion(this.GrainType);//标记将要处理的Version
                        this.SnapshotHandler.Apply(this.Snapshot, fullyEvent);
                        this.Snapshot.Base.UpdateVersion(fullyEvent.BasicInfo, this.GrainType);//更新处理完成的Version
                    }

                    if (eventList.Count < this.CoreOptions.NumberOfEventsPerRead)
                    {
                        break;
                    }
                }

                if (this.Snapshot.Base.Version - this.SnapshotEventVersion >= this.CoreOptions.MinSnapshotVersionInterval)
                {
                    var saveTask = this.SaveSnapshotAsync(true, true);
                    if (!saveTask.IsCompletedSuccessfully)
                    {
                        await saveTask;
                    }
                }

                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("Recovery completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "Recovery failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                throw;
            }
        }

        public override async Task OnDeactivateAsync()
        {
            try
            {
                if (this.Snapshot.Base.Version - this.SnapshotEventVersion >= this.CoreOptions.MinSnapshotVersionInterval)
                {
                    var saveTask = this.SaveSnapshotAsync(true, true);
                    if (!saveTask.IsCompletedSuccessfully)
                    {
                        await saveTask;
                    }

                    var onDeactivatedTask = this.OnDeactivated();
                    if (!onDeactivatedTask.IsCompletedSuccessfully)
                    {
                        await onDeactivatedTask;
                    }
                }

                if (this.ArchiveOptions.On && this.NewArchive != null)
                {
                    if (this.NewArchive.EndVersion - this.NewArchive.StartVersion >= this.ArchiveOptions.MinVersionIntervalAtDeactivate)
                    {
                        var archiveTask = this.Archive(true);
                        if (!archiveTask.IsCompletedSuccessfully)
                        {
                            await archiveTask;
                        }
                    }
                }

                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("Deactivate completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "Deactivate failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                throw;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnDeactivated() => Consts.ValueTaskDone;

        protected virtual async Task ReadSnapshotAsync()
        {
            try
            {
                //从快照中恢复状态
                this.Snapshot = await this.SnapshotStorage.Get(this.GrainId);

                if (this.Snapshot is null)
                {
                    //从归档中恢复状态
                    if (this.ArchiveOptions.On && this.LastArchive != null)
                    {
                        this.Snapshot = await this.ArchiveStorage.GetById(this.LastArchive.Id);
                    }

                    if (this.Snapshot is null)
                    {
                        //新建状态
                        var createTask = this.CreateSnapshot();
                        if (!createTask.IsCompletedSuccessfully)
                        {
                            await createTask;
                        }
                    }
                }

                this.SnapshotEventVersion = this.Snapshot.Base.Version;
                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("ReadSnapshot completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "ReadSnapshot failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                throw;
            }
        }

        protected async ValueTask SaveSnapshotAsync(bool force = false, bool isLatest = false)
        {
            if (this.Snapshot.Base.Version != this.Snapshot.Base.DoingVersion)
            {
                throw new StateInsecurityException(this.Snapshot.Base.StateId.ToString(), this.GrainType, this.Snapshot.Base.DoingVersion, this.Snapshot.Base.Version);
            }

            //如果版本号差超过设置则更新快照
            if ((force && this.Snapshot.Base.Version > this.SnapshotEventVersion) ||
                (this.Snapshot.Base.Version - this.SnapshotEventVersion >= this.CoreOptions.SnapshotVersionInterval))
            {
                var oldLatestMinEventTimestamp = this.Snapshot.Base.LatestMinEventTimestamp;
                try
                {
                    var onSaveSnapshotTask = this.OnStartSaveSnapshot();
                    if (!onSaveSnapshotTask.IsCompletedSuccessfully)
                    {
                        await onSaveSnapshotTask;
                    }

                    this.Snapshot.Base.LatestMinEventTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    this.Snapshot.Base.IsLatest = isLatest;
                    if (this.MetricMonitor != default)
                    {
                        var startTime = DateTimeOffset.UtcNow;
                        if (this.SnapshotEventVersion == 0)
                        {
                            await this.SnapshotStorage.Insert(this.Snapshot);
                        }
                        else
                        {
                            await this.SnapshotStorage.Update(this.Snapshot);
                        }

                        var metric = new SnapshotMetricElement
                        {
                            Actor = this.GrainType.Name,
                            ElapsedVersion = (int)(this.Snapshot.Base.Version - this.SnapshotEventVersion),
                            SaveElapsedMs = (int)DateTimeOffset.UtcNow.Subtract(startTime).TotalMilliseconds,
                            Snapshot = typeof(StateType).Name
                        };
                        this.MetricMonitor.Report(metric);
                    }
                    else
                    {
                        if (this.SnapshotEventVersion == 0)
                        {
                            await this.SnapshotStorage.Insert(this.Snapshot);
                        }
                        else
                        {
                            await this.SnapshotStorage.Update(this.Snapshot);
                        }
                    }

                    this.SnapshotEventVersion = this.Snapshot.Base.Version;
                    if (this.Logger.IsEnabled(LogLevel.Trace))
                    {
                        this.Logger.LogTrace("SaveSnapshot completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                    }
                }
                catch (Exception ex)
                {
                    this.Snapshot.Base.LatestMinEventTimestamp = oldLatestMinEventTimestamp;
                    this.Logger.LogCritical(ex, "SaveSnapshot failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                    throw;
                }
            }
        }

        protected async Task Over(OverType overType)
        {
            if (this.Snapshot.Base.IsOver)
            {
                throw new StateIsOverException(this.Snapshot.Base.StateId.ToString(), this.GrainType);
            }

            if (this.Snapshot.Base.Version != this.Snapshot.Base.DoingVersion)
            {
                throw new StateInsecurityException(this.Snapshot.Base.StateId.ToString(), this.GrainType, this.Snapshot.Base.DoingVersion, this.Snapshot.Base.Version);
            }

            if (overType != OverType.None)
            {
                var versions = await this.ObserverUnit.GetAndSaveVersion(this.Snapshot.Base.StateId, this.Snapshot.Base.Version);
                if (versions.Any(v => v < this.Snapshot.Base.Version))
                {
                    throw new ObserverNotCompletedException(this.GrainType.FullName, this.Snapshot.Base.StateId.ToString());
                }
            }

            this.Snapshot.Base.IsOver = true;
            this.Snapshot.Base.IsLatest = true;
            if (this.SnapshotEventVersion != this.Snapshot.Base.Version)
            {
                var saveTask = this.SaveSnapshotAsync(true, true);
                if (!saveTask.IsCompletedSuccessfully)
                {
                    await saveTask;
                }
            }
            else
            {
                await this.SnapshotStorage.Over(this.Snapshot.Base.StateId, true);
            }

            if (overType == OverType.ArchivingEvent)
            {
                if (this.ArchiveOptions.On)
                {
                    await this.DeleteAllArchive();
                }

                await this.ArchiveStorage.EventArichive(this.Snapshot.Base.StateId, this.Snapshot.Base.Version, this.Snapshot.Base.StartTimestamp);
            }
            else if (overType == OverType.DeleteEvent)
            {
                if (this.ArchiveOptions.On)
                {
                    await this.DeleteAllArchive();
                }

                await this.EventStorage.DeletePrevious(this.Snapshot.Base.StateId, this.Snapshot.Base.Version, this.Snapshot.Base.StartTimestamp);
            }
            else if (overType == OverType.DeleteAll)
            {
                if (this.ArchiveOptions.On)
                {
                    await this.DeleteAllArchive();
                }

                await this.EventStorage.DeletePrevious(this.Snapshot.Base.StateId, this.Snapshot.Base.Version, this.Snapshot.Base.StartTimestamp);

                if (this.SnapshotEventVersion > 0)
                {
                    await this.SnapshotStorage.Delete(this.GrainId);
                    this.SnapshotEventVersion = 0;
                }
            }
            else if (this.ArchiveOptions.On && this.BriefArchiveList.Count > 0)
            {
                await this.ArchiveStorage.Over(this.Snapshot.Base.StateId, true);
            }
        }

        private async Task DeleteArchive(string briefId)
        {
            await this.ArchiveStorage.Delete(this.GrainId, briefId);
            var task = this.OnStartDeleteArchive(briefId);
            if (!task.IsCompletedSuccessfully)
            {
                await task;
            }
        }

        protected async Task DeleteAllArchive()
        {
            if (this.BriefArchiveList.Count > 0)
            {
                var task = this.OnStartDeleteAllArchive();
                if (!task.IsCompletedSuccessfully)
                {
                    await task;
                }

                await this.ArchiveStorage.DeleteAll(this.GrainId);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnStartSaveSnapshot() => Consts.ValueTaskDone;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnStartDeleteArchive(string briefId) => Consts.ValueTaskDone;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnStartDeleteAllArchive() => Consts.ValueTaskDone;

        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask CreateSnapshot()
        {
            this.Snapshot = new Snapshot<PrimaryKey, StateType>(this.GrainId);
            return Consts.ValueTaskDone;
        }

        /// <summary>
        /// 删除状态
        /// </summary>
        /// <returns></returns>
        protected async ValueTask DeleteSnapshot()
        {
            if (this.Snapshot.Base.IsOver)
            {
                throw new StateIsOverException(this.Snapshot.Base.StateId.ToString(), this.GrainType);
            }

            if (this.SnapshotEventVersion > 0)
            {
                await this.SnapshotStorage.Delete(this.GrainId);
                this.SnapshotEventVersion = 0;
            }
        }

        protected async Task Reset()
        {
            await this.Over(OverType.DeleteAll);
            await this.RecoverySnapshot();
            await this.ObserverUnit.Reset(this.Snapshot.Base.StateId);
        }

        protected virtual async Task<bool> RaiseEvent(IEvent @event, EventUID uid = null)
        {
            if (this.Snapshot.Base.IsOver)
            {
                throw new StateIsOverException(this.Snapshot.Base.StateId.ToString(), this.GrainType);
            }

            try
            {
                var fullyEvent = new FullyEvent<PrimaryKey>
                {
                    Event = @event,
                    BasicInfo = new EventBasicInfo
                    {
                        Version = this.Snapshot.Base.Version + 1
                    },
                    StateId = this.Snapshot.Base.StateId
                };
                string unique = default;
                if (uid is null)
                {
                    fullyEvent.BasicInfo.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    unique = fullyEvent.GetEventId();
                }
                else
                {
                    fullyEvent.BasicInfo.Timestamp = uid.Timestamp;
                    unique = uid.UID;
                }

                var startTask = this.OnRaiseStart(fullyEvent);
                if (!startTask.IsCompletedSuccessfully)
                {
                    await startTask;
                }

                this.Snapshot.Base.IncrementDoingVersion(this.GrainType);//标记将要处理的Version
                var evtType = @event.GetType();
                using var baseBytes = fullyEvent.BasicInfo.ConvertToBytes();
                var typeCode = this.TypeFinder.GetCode(evtType);
                var evtBytes = this.Serializer.SerializeToUtf8Bytes(@event, evtType);
                bool appendResult;
                if (this.MetricMonitor != default)
                {
                    var startTime = DateTimeOffset.UtcNow;
                    appendResult = await this.EventStorage.Append(fullyEvent, Encoding.UTF8.GetString(evtBytes), unique);
                    var nowTime = DateTimeOffset.UtcNow;
                    this.MetricMonitor.Report(new EventMetricElement
                    {
                        Actor = this.GrainType.Name,
                        ActorId = this.GrainId.ToString(),
                        Event = evtType.Name,
                        FromEvent = uid?.FromEvent,
                        FromEventActor = uid?.FromActor,
                        InsertElapsedMs = (int)nowTime.Subtract(startTime).TotalMilliseconds,
                        IntervalPrevious = uid == default ? 0 : (int)(nowTime.ToUnixTimeMilliseconds() - uid.Timestamp),
                        Ignore = !appendResult,
                    });
                }
                else
                {
                    appendResult = await this.EventStorage.Append(fullyEvent, Encoding.UTF8.GetString(evtBytes), unique);
                }

                if (appendResult)
                {
                    this.SnapshotHandler.Apply(this.Snapshot, fullyEvent);
                    this.Snapshot.Base.UpdateVersion(fullyEvent.BasicInfo, this.GrainType);//更新处理完成的Version
                    var task = this.OnRaised(fullyEvent, new EventConverter(typeCode, this.Snapshot.Base.StateId, baseBytes.AsSpan(), evtBytes));
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

                    var saveSnapshotTask = this.SaveSnapshotAsync();
                    if (!saveSnapshotTask.IsCompletedSuccessfully)
                    {
                        await saveSnapshotTask;
                    }

                    using var buffer = new EventConverter(typeCode, this.Snapshot.Base.StateId, baseBytes.AsSpan(), evtBytes).ConvertToBytes();
                    await this.PublishToEventBus(buffer.AsSpan().ToArray(), this.GrainId.ToString());
                    if (this.Logger.IsEnabled(LogLevel.Trace))
                    {
                        this.Logger.LogTrace("RaiseEvent completed: {0}->{1}->{2}", this.GrainType.FullName, this.Serializer.Serialize(fullyEvent), this.Serializer.Serialize(this.Snapshot));
                    }

                    return true;
                }
                else
                {
                    if (this.Logger.IsEnabled(LogLevel.Trace))
                    {
                        this.Logger.LogTrace("RaiseEvent failed: {0}->{1}->{2}", this.GrainType.FullName, this.Serializer.Serialize(fullyEvent), this.Serializer.Serialize(this.Snapshot));
                    }

                    this.Snapshot.Base.DecrementDoingVersion();//还原doing Version
                    var task = this.OnRaiseFailed(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "RaiseEvent failed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                await this.RecoverySnapshot();//还原状态
                //出现错误可能会重复出现，所以把之前的快照进行更新，提高还原速度
                var saveSnapshotTask = this.SaveSnapshotAsync(true);
                if (!saveSnapshotTask.IsCompletedSuccessfully)
                {
                    await saveSnapshotTask;
                }

                throw;
            }

            return false;
        }

        //发送事件到EventBus中
        protected async Task PublishToEventBus(byte[] bytes, string hashKey)
        {
            if (this.ObserverEventHandlers.Count > 0)
            {
                try
                {
                    if (this.CoreOptions.PriorityAsyncEventBus)
                    {
                        try
                        {
                            var publishTask = this.EventBusProducer.Publish(bytes, hashKey);
                            if (!publishTask.IsCompletedSuccessfully)
                            {
                                await publishTask;
                            }
                        }
                        catch (Exception ex)
                        {
                            this.Logger.LogCritical(ex, "EventBus failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                            //当消息队列出现问题的时候同步推送
                            await Task.WhenAll(this.ObserverEventHandlers.Select(func => func(new BytesBox(bytes, null))));
                        }
                    }
                    else
                    {
                        try
                        {
                            await Task.WhenAll(this.ObserverEventHandlers.Select(func => func(new BytesBox(bytes, null))));
                        }
                        catch (Exception ex)
                        {
                            this.Logger.LogCritical(ex, "EventBus failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                            //当消息队列出现问题的时候异步推送
                            var publishTask = this.EventBusProducer.Publish(bytes, hashKey);
                            if (!publishTask.IsCompletedSuccessfully)
                            {
                                await publishTask;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    this.Logger.LogCritical(ex, "EventBus failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                }
            }
        }

        protected virtual async ValueTask OnRaiseStart(FullyEvent<PrimaryKey> @event)
        {
            if (this.Snapshot.Base.Version == 0)
            {
                return;
            }

            if (this.Snapshot.Base.IsLatest)
            {
                await this.SnapshotStorage.UpdateIsLatest(this.Snapshot.Base.StateId, false);
                this.Snapshot.Base.IsLatest = false;
            }

            if (this.ClearedArchive != null && @event.BasicInfo.Timestamp < this.ClearedArchive.StartTimestamp)
            {
                throw new EventIsClearedException(@event.GetType().FullName, this.Serializer.Serialize(@event, @event.GetType()), this.ClearedArchive.Index);
            }

            if (this.SnapshotEventVersion > 0)
            {
                if (@event.BasicInfo.Timestamp < this.Snapshot.Base.LatestMinEventTimestamp)
                {
                    await this.SnapshotStorage.UpdateLatestMinEventTimestamp(this.Snapshot.Base.StateId, @event.BasicInfo.Timestamp);
                }

                if (@event.BasicInfo.Timestamp < this.Snapshot.Base.StartTimestamp)
                {
                    await this.SnapshotStorage.UpdateStartTimestamp(this.Snapshot.Base.StateId, @event.BasicInfo.Timestamp);
                }
            }

            if (this.ArchiveOptions.On &&
                this.LastArchive != null &&
                @event.BasicInfo.Timestamp < this.LastArchive.EndTimestamp)
            {
                foreach (var archive in this.BriefArchiveList.Where(a => @event.BasicInfo.Timestamp < a.EndTimestamp && !a.EventIsCleared).OrderByDescending(v => v.Index))
                {
                    if (@event.BasicInfo.Timestamp < archive.EndTimestamp)
                    {
                        await this.DeleteArchive(archive.Id);
                        if (this.NewArchive != null)
                        {
                            this.NewArchive = this.CombineArchiveInfo(archive, this.NewArchive);
                        }
                        else
                        {
                            this.NewArchive = archive;
                        }

                        this.BriefArchiveList.Remove(archive);
                    }
                }

                this.LastArchive = this.BriefArchiveList.LastOrDefault();
            }
        }

        protected virtual ValueTask OnRaised(FullyEvent<PrimaryKey> @event, in EventConverter transport)
        {
            if (this.ArchiveOptions.On)
            {
                return this.EventArchive(@event);
            }

            return Consts.ValueTaskDone;
        }

        protected virtual ValueTask OnRaiseFailed(FullyEvent<PrimaryKey> @event)
        {
            if (this.ArchiveOptions.On && this.NewArchive != null)
            {
                return this.Archive();
            }

            return Consts.ValueTaskDone;
        }

        protected async ValueTask EventArchive(FullyEvent<PrimaryKey> @event)
        {
            if (this.NewArchive is null)
            {
                this.NewArchive = new ArchiveBrief
                {
                    Id = (await this.ServiceProvider.GetService<IGrainFactory>().GetGrain<IUtcUID>(this.GrainType.FullName).NewID()),
                    StartTimestamp = @event.BasicInfo.Timestamp,
                    StartVersion = @event.BasicInfo.Version,
                    Index = this.LastArchive != null ? this.LastArchive.Index + 1 : 0,
                    EndTimestamp = @event.BasicInfo.Timestamp,
                    EndVersion = @event.BasicInfo.Version
                };
            }
            else
            {
                //判定有没有时间戳小于前一个归档
                if (this.NewArchive.StartTimestamp == 0 || @event.BasicInfo.Timestamp < this.NewArchive.StartTimestamp)
                {
                    this.NewArchive.StartTimestamp = @event.BasicInfo.Timestamp;
                }

                if (@event.BasicInfo.Timestamp > this.NewArchive.StartTimestamp)
                {
                    this.NewArchive.EndTimestamp = @event.BasicInfo.Timestamp;
                }

                this.NewArchive.EndVersion = @event.BasicInfo.Version;
            }

            var archiveTask = this.Archive();
            if (!archiveTask.IsCompletedSuccessfully)
            {
                await archiveTask;
            }
        }

        private ArchiveBrief CombineArchiveInfo(ArchiveBrief main, ArchiveBrief merge)
        {
            if (merge.StartTimestamp < main.StartTimestamp)
            {
                main.StartTimestamp = merge.StartTimestamp;
            }

            if (merge.StartVersion < main.StartVersion)
            {
                main.StartVersion = merge.StartVersion;
            }

            if (merge.EndTimestamp > main.EndTimestamp)
            {
                main.EndTimestamp = merge.EndTimestamp;
            }

            if (merge.EndVersion > main.EndVersion)
            {
                main.EndVersion = merge.EndVersion;
            }

            return main;
        }

        protected async ValueTask Archive(bool force = false)
        {
            if (this.Snapshot.Base.Version != this.Snapshot.Base.DoingVersion)
            {
                throw new StateInsecurityException(this.Snapshot.Base.StateId.ToString(), this.GrainType, this.Snapshot.Base.DoingVersion, this.Snapshot.Base.Version);
            }

            if (force || this.NewArchive.IsCompletedArchive(this.ArchiveOptions, this.LastArchive))
            {
                var task = this.OnStartArchive();
                if (!task.IsCompletedSuccessfully)
                {
                    await task;
                }

                await this.ArchiveStorage.Insert(this.NewArchive, this.Snapshot);
                this.BriefArchiveList.Add(this.NewArchive);
                this.LastArchive = this.NewArchive;
                this.NewArchive = default;
                var onTask = this.OnArchiveCompleted();
                if (!onTask.IsCompletedSuccessfully)
                {
                    await onTask;
                }
            }
        }

        protected virtual async ValueTask OnArchiveCompleted()
        {
            //开始执行事件清理逻辑
            var noCleareds = this.BriefArchiveList.Where(a => !a.EventIsCleared).ToList();
            if (noCleareds.Count >= this.ArchiveOptions.MaxSnapshotArchiveRecords)
            {
                var minArchive = noCleareds.FirstOrDefault();
                if (minArchive != null)
                {
                    //判断需要清理的event是否都被Observer执行过
                    var versions = await this.ObserverUnit.GetAndSaveVersion(this.Snapshot.Base.StateId, this.Snapshot.Base.Version);
                    if (versions.All(v => v >= minArchive.EndVersion))
                    {
                        //清理归档对应的事件
                        await this.ArchiveStorage.EventIsClear(this.Snapshot.Base.StateId, minArchive.Id);
                        minArchive.EventIsCleared = true;
                        //如果快照的版本小于需要清理的最大事件版本号，则保存快照
                        if (this.SnapshotEventVersion < minArchive.EndVersion)
                        {
                            var saveTask = this.SaveSnapshotAsync(true);
                            if (!saveTask.IsCompletedSuccessfully)
                            {
                                await saveTask;
                            }
                        }

                        if (this.ArchiveOptions.EventArchiveType == EventArchiveType.Delete)
                        {
                            await this.EventStorage.DeletePrevious(this.Snapshot.Base.StateId, minArchive.EndVersion, this.Snapshot.Base.StartTimestamp);
                        }
                        else
                        {
                            await this.ArchiveStorage.EventArichive(this.Snapshot.Base.StateId, minArchive.EndVersion, this.Snapshot.Base.StartTimestamp);
                        }

                        this.ClearedArchive = minArchive;
                        //只保留一个清理过事件的快照，其它的删除掉
                        var cleareds = this.BriefArchiveList.Where(a => a.EventIsCleared).OrderBy(a => a.Index).ToArray();
                        if (cleareds.Length > 1)
                        {
                            for (int i = 0; i < cleareds.Length - 1; i++)
                            {
                                await this.DeleteArchive(cleareds[i].Id);
                                this.BriefArchiveList.Remove(cleareds[i]);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 当状态过于复杂，需要自定义归档逻辑的时候使用该方法
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask OnStartArchive() => Consts.ValueTaskDone;

        /// <summary>
        /// 发送无状态更改的消息到消息队列
        /// </summary>
        /// <returns></returns>
        protected async ValueTask Publish<T>(T msg, string hashKey = null)
        {
            if (string.IsNullOrEmpty(hashKey))
            {
                hashKey = this.GrainId.ToString();
            }

            try
            {
                var wrapper = new TransportMessage(this.TypeFinder.GetCode(msg.GetType()), this.Serializer.SerializeToUtf8Bytes(msg, msg.GetType()));
                using var array = wrapper.ConvertToBytes();
                var pubLishTask = this.EventBusProducer.Publish(array.AsSpan().ToArray(), hashKey);
                if (!pubLishTask.IsCompletedSuccessfully)
                {
                    await pubLishTask;
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "EventBus failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                throw;
            }
        }
    }
}
