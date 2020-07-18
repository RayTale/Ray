using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Abstractions;
using Ray.Core.Abstractions.Monitor;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Observer;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Core
{
    public abstract class ShadowGrain<PrimaryKey, Main, StateType> : Grain, IObserver
        where StateType : class, new()
    {
        public ShadowGrain()
        {
            this.GrainType = this.GetType();
            if (typeof(ICloneable<StateType>).IsAssignableFrom(typeof(StateType)))
            {
                this.IsTxShadow = typeof(TxGrain<,>).MakeGenericType(typeof(PrimaryKey), typeof(StateType)).IsAssignableFrom(typeof(Main));
            }
            else
            {
                this.IsTxShadow = false;
            }
        }

        protected bool IsTxShadow { get; }

        protected CoreOptions CoreOptions { get; private set; }

        protected ArchiveOptions ArchiveOptions { get; private set; }

        protected ILogger Logger { get; private set; }

        protected ISerializer Serializer { get; private set; }

        protected ITypeFinder TypeFinder { get; private set; }

        protected Snapshot<PrimaryKey, StateType> Snapshot { get; set; }
        private PrimaryKey _GrainId;
        private bool _GrainIdAcquired = false;
        /// <summary>
        /// Primary key of actor
        /// Because there are multiple types, dynamic assignment in OnActivateAsync
        /// </summary>
        public PrimaryKey GrainId { get; private set; }
        /// <summary>
        /// 分批次批量读取事件的时候每次读取的数据量
        /// </summary>
        protected virtual int NumberOfEventsPerRead => this.CoreOptions.NumberOfEventsPerRead;

        /// <summary>
        /// 是否全量激活，true代表启动时会执行大于快照版本的所有事件,false代表更快的启动，后续有事件进入的时候再处理大于快照版本的事件
        /// </summary>
        protected virtual bool FullyActive => false;

        /// <summary>
        /// Grain的Type
        /// </summary>
        protected Type GrainType { get; }

        /// <summary>
        /// 指标收集器
        /// </summary>
        protected IMetricMonitor MetricMonitor { get; private set; }

        /// <summary>
        /// 所在的组
        /// </summary>
        protected string Group { get; private set; }

        /// <summary>
        /// 事件存储器
        /// </summary>
        protected IEventStorage<PrimaryKey> EventStorage { get; private set; }

        /// <summary>
        /// 状态存储器
        /// </summary>
        protected ISnapshotStorage<PrimaryKey, StateType> SnapshotStorage { get; private set; }

        /// <summary>
        /// 归档存储器
        /// </summary>
        protected IArchiveStorage<PrimaryKey, StateType> ArchiveStorage { get; private set; }

        protected ISnapshotHandler<PrimaryKey, StateType> SnapshotHandler { get; private set; }

        protected ArchiveBrief LastArchive { get; private set; }
        #region 初始化数据

        /// <summary>
        /// 依赖注入统一方法
        /// </summary>
        /// <returns></returns>
        protected async virtual ValueTask DependencyInjection()
        {
            this.Logger = (ILogger)this.ServiceProvider.GetService(typeof(ILogger<>).MakeGenericType(this.GrainType));
            this.Group = this.ServiceProvider.GetService<IObserverUnitContainer>().GetUnit<PrimaryKey>(typeof(Main)).GetGroup(this.GrainType);
            this.MetricMonitor = this.ServiceProvider.GetService<IMetricMonitor>();
            this.CoreOptions = this.ServiceProvider.GetOptionsByName<CoreOptions>(typeof(Main).FullName);
            this.TypeFinder = this.ServiceProvider.GetService<ITypeFinder>();
            this.ArchiveOptions = this.ServiceProvider.GetOptionsByName<ArchiveOptions>(typeof(Main).FullName);
            this.Serializer = this.ServiceProvider.GetService<ISerializer>();
            this.SnapshotHandler = this.ServiceProvider.GetService<ISnapshotHandler<PrimaryKey, StateType>>();
            if (this.SnapshotHandler == default)
            {
                throw new UnfindSnapshotHandlerException(this.GrainType);
            }

            var configureBuilder = this.ServiceProvider.GetService<IConfigureBuilder<PrimaryKey, Main>>();
            var storageConfigTask = configureBuilder.GetConfig(this.ServiceProvider, this.GrainId);
            if (!storageConfigTask.IsCompletedSuccessfully)
            {
                await storageConfigTask;
            }

            var storageFactory = this.ServiceProvider.GetService(configureBuilder.StorageFactory) as IStorageFactory;
            //创建归档存储器
            var archiveStorageTask = storageFactory.CreateArchiveStorage<PrimaryKey, StateType>(storageConfigTask.Result, this.GrainId);
            if (!archiveStorageTask.IsCompletedSuccessfully)
            {
                await archiveStorageTask;
            }

            this.ArchiveStorage = archiveStorageTask.Result;
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
        }

        public override async Task OnActivateAsync()
        {
            var type = typeof(PrimaryKey);
            if (type == typeof(long) && this.GetPrimaryKeyLong() is PrimaryKey longKey)
                GrainId = longKey;
            else if (type == typeof(string) && this.GetPrimaryKeyString() is PrimaryKey stringKey)
                GrainId = stringKey;
            else if (type == typeof(Guid) && this.GetPrimaryKey() is PrimaryKey guidKey)
                GrainId = guidKey;
            else
                throw new ArgumentOutOfRangeException(typeof(PrimaryKey).FullName);
            var dITask = DependencyInjection();
            if (!dITask.IsCompletedSuccessfully)
            {
                await dITask;
            }

            try
            {
                if (this.ArchiveOptions.On)
                {
                    //加载最后一条归档
                    this.LastArchive = await this.ArchiveStorage.GetLatestBrief(this.GrainId);
                }

                await this.ReadSnapshotAsync();
                if (this.FullyActive)
                {
                    await this.RecoveryFromStorage();
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

        /// <summary>
        /// 从库里恢复
        /// </summary>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        private async Task RecoveryFromStorage()
        {
            while (true)
            {
                var eventList = await this.EventStorage.GetList(this.GrainId, this.Snapshot.Base.StartTimestamp, this.Snapshot.Base.Version + 1, this.Snapshot.Base.Version + this.NumberOfEventsPerRead);
                var task = this.Tell(eventList);
                if (!task.IsCompletedSuccessfully)
                {
                    await task;
                }

                if (eventList.Count < this.NumberOfEventsPerRead)
                {
                    break;
                }
            }

        }

        protected virtual async ValueTask Tell(IEnumerable<FullyEvent<PrimaryKey>> eventList)
        {
            foreach (var @event in eventList)
            {
                var task = this.Tell(@event);
                if (!task.IsCompletedSuccessfully)
                {
                    await task;
                }
            }
        }

        protected virtual async Task ReadSnapshotAsync()
        {
            try
            {
                this.Snapshot = await this.SnapshotStorage.Get(this.GrainId);
                if (this.Snapshot is null)
                {
                    //从归档中恢复状态
                    if (this.ArchiveOptions.On && this.LastArchive != null)
                    {
                        this.Snapshot = await this.ArchiveStorage.GetById(this.LastArchive.Id);
                    }
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
                else if (this.IsTxShadow)
                {
                    this.Snapshot = new TxSnapshot<PrimaryKey, StateType>()
                    {
                        Base = new TxSnapshotBase<PrimaryKey>(this.Snapshot.Base),
                        State = this.Snapshot.State
                    };
                }

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

        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask CreateSnapshot()
        {
            if (this.IsTxShadow)
            {
                this.Snapshot = new TxSnapshot<PrimaryKey, StateType>(this.GrainId);
            }
            else
            {
                this.Snapshot = new Snapshot<PrimaryKey, StateType>(this.GrainId);
            }

            return Consts.ValueTaskDone;
        }

        #endregion
        public Task OnNext(Immutable<byte[]> bytes)
        {
            if (EventConverter.TryParseWithNoId(bytes.Value, out var transport))
            {
                var eventType = this.TypeFinder.FindType(transport.EventUniqueName);
                var data = this.Serializer.Deserialize(transport.EventBytes, eventType);
                if (data is IEvent @event)
                {
                    var eventBase = transport.BaseBytes.ParseToEventBase();
                    if (eventBase.Version > this.Snapshot.Base.Version)
                    {
                        var tellTask = this.Tell(new FullyEvent<PrimaryKey>
                        {
                            StateId = this.GrainId,
                            BasicInfo = eventBase,
                            Event = @event
                        });
                        if (!tellTask.IsCompletedSuccessfully)
                        {
                            return tellTask.AsTask();
                        }
                    }

                    if (this.Logger.IsEnabled(LogLevel.Trace))
                    {
                        this.Logger.LogTrace("OnNext completed: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(data, eventType));
                    }
                }
                else
                {
                    if (this.Logger.IsEnabled(LogLevel.Trace))
                    {
                        this.Logger.LogTrace("Non-Event: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(data, eventType));
                    }
                }
            }

            return Task.CompletedTask;
        }

        public async Task OnNext(Immutable<List<byte[]>> items)
        {
            var events = items.Value.Select(bytes =>
            {
                if (EventConverter.TryParseWithNoId(bytes, out var transport))
                {
                    var eventType = this.TypeFinder.FindType(transport.EventUniqueName);
                    var data = this.Serializer.Deserialize(transport.EventBytes, eventType);
                    if (data is IEvent @event)
                    {
                        var eventBase = transport.BaseBytes.ParseToEventBase();
                        if (eventBase.Version > this.Snapshot.Base.Version)
                        {
                            return new FullyEvent<PrimaryKey>
                            {
                                StateId = this.GrainId,
                                BasicInfo = eventBase,
                                Event = @event
                            };
                        }
                    }
                    else
                    {
                        if (this.Logger.IsEnabled(LogLevel.Trace))
                        {
                            this.Logger.LogTrace("Non-Event: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(data, eventType));
                        }
                    }
                }

                return default;
            }).Where(o => o != null).OrderBy(o => o.BasicInfo.Version).ToList();
            foreach (var evt in events)
            {
                var tellTask = this.Tell(evt);
                if (!tellTask.IsCompletedSuccessfully)
                {
                    await tellTask;
                }

                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("OnNext completed: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(evt));
                }
            }
        }

        public Task<long> GetVersion()
        {
            return Task.FromResult(this.Snapshot.Base.Version);
        }

        public Task<long> GetAndSaveVersion(long compareVersion)
        {
            return Task.FromResult(this.Snapshot.Base.Version);
        }

        public async Task<bool> SyncFromObservable(long compareVersion)
        {
            if (this.Snapshot.Base.Version < compareVersion)
            {
                await this.RecoveryFromStorage();
            }

            return this.Snapshot.Base.Version == compareVersion;
        }

        protected async ValueTask Tell(FullyEvent<PrimaryKey> @event)
        {
            if (@event.BasicInfo.Version == this.Snapshot.Base.Version + 1)
            {
                var onEventDeliveredTask = this.EventDelivered(@event);
                if (!onEventDeliveredTask.IsCompletedSuccessfully)
                {
                    await onEventDeliveredTask;
                }

                this.Snapshot.Base.FullUpdateVersion(@event.BasicInfo, this.GrainType);//更新处理完成的Version
            }
            else if (@event.BasicInfo.Version > this.Snapshot.Base.Version)
            {
                var eventList = await this.EventStorage.GetList(this.GrainId, this.Snapshot.Base.StartTimestamp, this.Snapshot.Base.Version + 1, @event.BasicInfo.Version - 1);
                foreach (var evt in eventList)
                {
                    var onEventDeliveredTask = this.EventDelivered(evt);
                    if (!onEventDeliveredTask.IsCompletedSuccessfully)
                    {
                        await onEventDeliveredTask;
                    }

                    this.Snapshot.Base.FullUpdateVersion(evt.BasicInfo, this.GrainType);//更新处理完成的Version
                }
            }

            if (@event.BasicInfo.Version == this.Snapshot.Base.Version + 1)
            {
                var onEventDeliveredTask = this.EventDelivered(@event);
                if (!onEventDeliveredTask.IsCompletedSuccessfully)
                {
                    await onEventDeliveredTask;
                }

                this.Snapshot.Base.FullUpdateVersion(@event.BasicInfo, this.GrainType);//更新处理完成的Version
            }

            if (@event.BasicInfo.Version > this.Snapshot.Base.Version)
            {
                throw new EventVersionUnorderedException(this.GrainId.ToString(), this.GrainType, @event.BasicInfo.Version, this.Snapshot.Base.Version);
            }
        }

        private async ValueTask EventDelivered(FullyEvent<PrimaryKey> fullyEvent)
        {
            try
            {
                if (this.MetricMonitor != default)
                {
                    var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    this.SnapshotHandler.Apply(this.Snapshot, fullyEvent);
                    var task = this.OnEventDelivered(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

                    this.MetricMonitor.Report(new FollowMetricElement
                    {
                        Actor = this.GrainType.Name,
                        Event = fullyEvent.Event.GetType().Name,
                        FromActor = typeof(Main).Name,
                        ElapsedMs = (int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - startTime),
                        DeliveryElapsedMs = (int)(startTime - fullyEvent.BasicInfo.Timestamp),
                        Group = this.Group
                    });
                }
                else
                {
                    this.SnapshotHandler.Apply(this.Snapshot, fullyEvent);
                    var task = this.OnEventDelivered(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "Delivered failed: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(fullyEvent, fullyEvent.Event.GetType()));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnEventDelivered(FullyEvent<PrimaryKey> @event) => Consts.ValueTaskDone;

        public virtual Task Reset()
        {
            return this.ReadSnapshotAsync();
        }
    }
}
