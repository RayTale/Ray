﻿using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Ray.Core
{
    public abstract class ShadowGrain<Main, PrimaryKey, StateType> : Grain, IObserver
        where StateType : class, new()
    {
        public ShadowGrain()
        {
            GrainType = GetType();
            if (typeof(IConcurrentObserver).IsAssignableFrom(GrainType))
                throw new NotSupportedException("ShadowGrain not supported inheritance from 'IConcurrentObserver'");

            if (typeof(ICloneable<StateType>).IsAssignableFrom(typeof(StateType)))
            {
                IsTxShadow = typeof(TxGrain<,>).MakeGenericType(typeof(PrimaryKey), typeof(StateType)).IsAssignableFrom(typeof(Main));
            }
            else
            {
                IsTxShadow = false;
            }
        }
        protected bool IsTxShadow { get; }
        protected CoreOptions CoreOptions { get; private set; }
        protected ArchiveOptions ArchiveOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected ISerializer Serializer { get; private set; }
        protected Snapshot<PrimaryKey, StateType> Snapshot { get; set; }
        private PrimaryKey _GrainId;
        private bool _GrainIdAcquired = false;
        public PrimaryKey GrainId
        {
            get
            {
                if (!_GrainIdAcquired)
                {
                    var type = typeof(PrimaryKey);
                    if (type == typeof(long) && this.GetPrimaryKeyLong() is PrimaryKey longKey)
                        _GrainId = longKey;
                    else if (type == typeof(string) && this.GetPrimaryKeyString() is PrimaryKey stringKey)
                        _GrainId = stringKey;
                    else if (type == typeof(Guid) && this.GetPrimaryKey() is PrimaryKey guidKey)
                        _GrainId = guidKey;
                    else
                        throw new ArgumentOutOfRangeException(typeof(PrimaryKey).FullName);
                    _GrainIdAcquired = true;
                }
                return _GrainId;
            }
        }
        /// <summary>
        /// 分批次批量读取事件的时候每次读取的数据量
        /// </summary>
        protected virtual int NumberOfEventsPerRead => CoreOptions.NumberOfEventsPerRead;
        /// <summary>
        /// 事件处理的超时时间
        /// </summary>
        protected virtual int EventAsyncProcessTimeoutSeconds => CoreOptions.EventAsyncProcessSecondsTimeout;
        /// <summary>
        /// 是否全量激活，true代表启动时会执行大于快照版本的所有事件,false代表更快的启动，后续有事件进入的时候再处理大于快照版本的事件
        /// </summary>
        protected virtual bool FullyActive => false;
        /// <summary>
        /// Grain的Type
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
        /// 归档存储器
        /// </summary>
        protected IArchiveStorage<PrimaryKey, StateType> ArchiveStorage { get; private set; }
        protected ISnapshotHandler<PrimaryKey, StateType> SnapshotHandler { get; private set; }
        protected ArchiveBrief LastArchive { get; private set; }
        #region 初始化数据
        /// <summary>
        /// 依赖注入统一方法
        /// </summary>
        protected async virtual ValueTask DependencyInjection()
        {
            Logger = (ILogger)ServiceProvider.GetService(typeof(ILogger<>).MakeGenericType(GrainType));
            CoreOptions = ServiceProvider.GetOptionsByName<CoreOptions>(typeof(Main).FullName);
            ArchiveOptions = ServiceProvider.GetOptionsByName<ArchiveOptions>(typeof(Main).FullName);
            Serializer = ServiceProvider.GetService<ISerializer>();
            SnapshotHandler = ServiceProvider.GetService<ISnapshotHandler<PrimaryKey, StateType>>();
            var configureBuilder = ServiceProvider.GetService<IConfigureBuilder<PrimaryKey, Main>>();
            var storageConfigTask = configureBuilder.GetConfig(ServiceProvider, GrainId);
            if (!storageConfigTask.IsCompletedSuccessfully)
                await storageConfigTask;
            var storageFactory = ServiceProvider.GetService(configureBuilder.StorageFactory) as IStorageFactory;
            //创建归档存储器
            var archiveStorageTask = storageFactory.CreateArchiveStorage<PrimaryKey, StateType>(storageConfigTask.Result, GrainId);
            if (!archiveStorageTask.IsCompletedSuccessfully)
                await archiveStorageTask;
            ArchiveStorage = archiveStorageTask.Result;
            //创建事件存储器
            var eventStorageTask = storageFactory.CreateEventStorage(storageConfigTask.Result, GrainId);
            if (!eventStorageTask.IsCompletedSuccessfully)
                await eventStorageTask;
            EventStorage = eventStorageTask.Result;
            //创建状态存储器
            var stateStorageTask = storageFactory.CreateSnapshotStorage<PrimaryKey, StateType>(storageConfigTask.Result, GrainId);
            if (!stateStorageTask.IsCompletedSuccessfully)
                await stateStorageTask;
            SnapshotStorage = stateStorageTask.Result;
        }
        public override async Task OnActivateAsync()
        {
            var dITask = DependencyInjection();
            if (!dITask.IsCompletedSuccessfully)
                await dITask;
            try
            {
                if (ArchiveOptions.On)
                {
                    //加载最后一条归档
                    LastArchive = await ArchiveStorage.GetLatestBrief(GrainId);
                }
                await ReadSnapshotAsync();
                if (FullyActive)
                {
                    while (true)
                    {
                        var eventList = await EventStorage.GetList(GrainId, Snapshot.Base.StartTimestamp, Snapshot.Base.Version + 1, Snapshot.Base.Version + NumberOfEventsPerRead);
                        var task = Tell(eventList);
                        if (!task.IsCompletedSuccessfully)
                            await task;
                        if (eventList.Count < NumberOfEventsPerRead) break;
                    };
                }
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Activation failed: {0}", GrainId.ToString());
                throw;
            }
        }
        protected virtual async ValueTask Tell(IEnumerable<IFullyEvent<PrimaryKey>> eventList)
        {
            foreach (var @event in eventList)
            {
                var task = Tell(@event);
                if (!task.IsCompletedSuccessfully)
                    await task;
            }
        }
        protected virtual async Task ReadSnapshotAsync()
        {
            try
            {
                Snapshot = await SnapshotStorage.Get(GrainId);
                if (Snapshot == default)
                {
                    //从归档中恢复状态
                    if (ArchiveOptions.On && LastArchive != default)
                    {
                        Snapshot = await ArchiveStorage.GetById(LastArchive.Id);
                    }
                }
                if (Snapshot == default)
                {
                    //新建状态
                    var createTask = CreateSnapshot();
                    if (!createTask.IsCompletedSuccessfully)
                        await createTask;
                }
                else if (IsTxShadow)
                {
                    Snapshot = new TxSnapshot<PrimaryKey, StateType>()
                    {
                        Base = new TxSnapshotBase<PrimaryKey>(Snapshot.Base),
                        State = Snapshot.State
                    };
                }
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Snapshot load completion:{0}", Serializer.Serialize(Snapshot));
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(ex, "Snapshot load failed", GrainId.ToString());
                throw;
            }
        }
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask CreateSnapshot()
        {
            if (IsTxShadow)
            {
                Snapshot = new TxSnapshot<PrimaryKey, StateType>(GrainId);
            }
            else
            {
                Snapshot = new Snapshot<PrimaryKey, StateType>(GrainId);
            }
            return Consts.ValueTaskDone;
        }
        #endregion
        public Task OnNext(Immutable<byte[]> bytes)
        {
            var (success, transport) = EventBytesTransport.FromBytesWithNoId(bytes.Value);
            if (success)
            {
                var eventType = TypeContainer.GetType(transport.EventTypeCode);
                var data = Serializer.Deserialize(transport.EventBytes, eventType);
                if (data is IEvent @event)
                {
                    var eventBase = EventBase.FromBytes(transport.BaseBytes);
                    if (eventBase.Version > Snapshot.Base.Version)
                    {
                        var tellTask = Tell(new FullyEvent<PrimaryKey>
                        {
                            StateId = GrainId,
                            Base = eventBase,
                            Event = @event
                        });
                        if (!tellTask.IsCompletedSuccessfully)
                            return tellTask.AsTask();
                    }
                }
                else
                {
                    if (Logger.IsEnabled(LogLevel.Information))
                        Logger.LogInformation("Non-event messages:{0}({1})", eventType, Serializer.Serialize(data));
                }
            }
            return Task.CompletedTask;
        }
        public Task<long> GetVersion()
        {
            return Task.FromResult(Snapshot.Base.Version);
        }
        public Task<long> GetAndSaveVersion(long compareVersion)
        {
            return Task.FromResult(Snapshot.Base.Version);
        }
        protected async ValueTask Tell(IFullyEvent<PrimaryKey> @event)
        {
            try
            {
                if (@event.Base.Version == Snapshot.Base.Version + 1)
                {
                    var onEventDeliveredTask = OnEventDelivered(@event);
                    if (!onEventDeliveredTask.IsCompletedSuccessfully)
                        await onEventDeliveredTask;
                    Snapshot.Base.FullUpdateVersion(@event.Base, GrainType);//更新处理完成的Version
                }
                else if (@event.Base.Version > Snapshot.Base.Version)
                {
                    var eventList = await EventStorage.GetList(GrainId, Snapshot.Base.StartTimestamp, Snapshot.Base.Version + 1, @event.Base.Version - 1);
                    foreach (var evt in eventList)
                    {
                        var onEventDeliveredTask = OnEventDelivered(evt);
                        if (!onEventDeliveredTask.IsCompletedSuccessfully)
                            await onEventDeliveredTask;
                        Snapshot.Base.FullUpdateVersion(evt.Base, GrainType);//更新处理完成的Version
                    }
                }
                if (@event.Base.Version == Snapshot.Base.Version + 1)
                {
                    var onEventDeliveredTask = OnEventDelivered(@event);
                    if (!onEventDeliveredTask.IsCompletedSuccessfully)
                        await onEventDeliveredTask;
                    Snapshot.Base.FullUpdateVersion(@event.Base, GrainType);//更新处理完成的Version
                }
                if (@event.Base.Version > Snapshot.Base.Version)
                {
                    throw new EventVersionUnorderedException(GrainId.ToString(), GrainType, @event.Base.Version, Snapshot.Base.Version);
                }
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "{0}({1})", @event.GetType().FullName, Serializer.Serialize(@event, @event.GetType()));
                throw;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnEventDelivered(IFullyEvent<PrimaryKey> @event)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("OnEventDelivered: {0}({1})", @event.GetType().FullName, Serializer.Serialize(@event, @event.GetType()));
            SnapshotHandler.Apply(Snapshot, @event);
            return Consts.ValueTaskDone;
        }
    }
}
