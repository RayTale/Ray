using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Core
{
    public abstract class ObserverGrain<MainGrain, PrimaryKey> : Grain, IObserver
    {
        public ObserverGrain(ILogger logger)
        {
            Logger = logger;
            GrainType = GetType();
        }
        public abstract PrimaryKey GrainId { get; }
        protected CoreOptions ConfigOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected ISerializer Serializer { get; private set; }
        /// <summary>
        /// Memory state, restored by snapshot + Event play or replay
        /// </summary>
        protected ObserverSnapshot<PrimaryKey> Snapshot { get; set; }
        /// <summary>
        /// 是否需要保存快照
        /// </summary>
        protected virtual bool SaveSnapshot => true;
        /// <summary>
        /// 是否全量激活，true代表启动时会执行大于快照版本的所有事件,false代表更快的启动，后续有事件进入的时候再处理大于快照版本的事件
        /// </summary>
        protected virtual bool FullyActive => false;
        /// <summary>
        /// 快照的事件版本号
        /// </summary>
        protected long SnapshotEventVersion { get; set; }
        /// <summary>
        /// 是否开启事件并发处理
        /// </summary>
        protected virtual bool EventConcurrentProcessing => false;
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
        protected IObserverSnapshotStorage<PrimaryKey> ObserverSnapshotStorage { get; private set; }
        #region 初始化数据
        /// <summary>
        /// 依赖注入统一方法
        /// </summary>
        protected async virtual ValueTask DependencyInjection()
        {
            ConfigOptions = ServiceProvider.GetOptionsByName<CoreOptions>(typeof(MainGrain).FullName);
            Serializer = ServiceProvider.GetService<ISerializer>();
            var configureBuilder = ServiceProvider.GetService<IConfigureBuilder<PrimaryKey, MainGrain>>();
            var storageConfigTask = configureBuilder.GetConfig(ServiceProvider, GrainId);
            if (!storageConfigTask.IsCompletedSuccessfully)
                await storageConfigTask;
            var storageFactory = ServiceProvider.GetService(configureBuilder.StorageFactory) as IStorageFactory;
            //创建事件存储器
            var eventStorageTask = storageFactory.CreateEventStorage(storageConfigTask.Result, GrainId);
            if (!eventStorageTask.IsCompletedSuccessfully)
                await eventStorageTask;
            EventStorage = eventStorageTask.Result;
            //创建状态存储器
            var followConfigTask = configureBuilder.GetObserverConfig(ServiceProvider,GrainType, GrainId);
            if (!followConfigTask.IsCompletedSuccessfully)
                await followConfigTask;
            var stateStorageTask = storageFactory.CreateObserverSnapshotStorage(followConfigTask.Result, GrainId);
            if (!stateStorageTask.IsCompletedSuccessfully)
                await stateStorageTask;
            ObserverSnapshotStorage = stateStorageTask.Result;
        }
        public override async Task OnActivateAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start activation followgrain with id = {0}", GrainId.ToString());
            var dITask = DependencyInjection();
            if (!dITask.IsCompletedSuccessfully)
                await dITask;
            try
            {
                await ReadSnapshotAsync();
                if (FullyActive)
                {
                    await FullActive();
                }
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Followgrain activation completed with id = {0}", GrainId.ToString());
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Followgrain activation failed with Id = {0}", GrainId.ToString());
                throw;
            }
        }
        private async Task FullActive()
        {
            while (true)
            {
                var eventList = await EventStorage.GetList(GrainId, Snapshot.StartTimestamp, Snapshot.Version + 1, Snapshot.Version + ConfigOptions.NumberOfEventsPerRead);
                if (EventConcurrentProcessing)
                {
                    await Task.WhenAll(eventList.Select(@event =>
                    {
                        var task = OnEventDelivered(@event);
                        if (!task.IsCompletedSuccessfully)
                            return task.AsTask();
                        else
                            return Task.CompletedTask;
                    }));
                    var lastEvt = eventList.Last();
                    Snapshot.UnsafeUpdateVersion(lastEvt.Base);
                }
                else
                {
                    foreach (var @event in eventList)
                    {
                        Snapshot.IncrementDoingVersion(GrainType);//标记将要处理的Version
                        var task = OnEventDelivered(@event);
                        if (!task.IsCompletedSuccessfully)
                            await task;
                        Snapshot.UpdateVersion(@event.Base, GrainType);//更新处理完成的Version
                    }
                }
                var saveTask = SaveSnapshotAsync();
                if (!saveTask.IsCompletedSuccessfully)
                    await saveTask;
                if (eventList.Count < ConfigOptions.NumberOfEventsPerRead) break;
            };
        }
        public override Task OnDeactivateAsync()
        {
            var needSaveSnap = Snapshot.Version - SnapshotEventVersion >= 1;
            if (Logger.IsEnabled(LogLevel.Information))
                Logger.LogInformation("Followgrain start deactivation with id = {0} ,{1}", GrainId.ToString(), needSaveSnap ? "updated snapshot" : "no update snapshot");
            if (needSaveSnap)
                return SaveSnapshotAsync(true).AsTask();
            else
                return Task.CompletedTask;
        }
        protected virtual async Task ReadSnapshotAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start read snapshot  with Id = {0} ,state version = {1}", GrainId.ToString(), Snapshot.Version);
            try
            {
                Snapshot = await ObserverSnapshotStorage.Get(GrainId);
                if (Snapshot == null)
                {
                    var createTask = InitFirstSnapshot();
                    if (!createTask.IsCompletedSuccessfully)
                        await createTask;
                }
                SnapshotEventVersion = Snapshot.Version;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("The snapshot of id = {0} read completed, state version = {1}", GrainId.ToString(), Snapshot.Version);
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "The snapshot of id = {0} read failed", GrainId.ToString());
                throw;
            }
        }
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask InitFirstSnapshot()
        {
            Snapshot = new ObserverSnapshot<PrimaryKey>
            {
                StateId = GrainId
            };
            return Consts.ValueTaskDone;
        }
        #endregion
        public Task OnNext(byte[] bytes)
        {
            var (success, transport) = EventBytesTransport.FromBytesWithNoId(bytes);
            if (success)
            {
                var data = Serializer.Deserialize(TypeContainer.GetType(transport.EventType), transport.EventBytes);
                if (data is IEvent @event)
                {
                    var eventBase = EventBase.FromBytes(transport.BaseBytes);
                    if (eventBase.Version > Snapshot.Version)
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
                        Logger.LogInformation("Receive non-event messages, grain Id = {0} ,message type = {1}", GrainId.ToString(), transport.EventType);
                }
            }
            return Task.CompletedTask;
        }
        public Task<long> GetVersion()
        {
            return Task.FromResult(Snapshot.Version);
        }
        public async Task<long> GetAndSaveVersion(long compareVersion)
        {
            if (SnapshotEventVersion < compareVersion && Snapshot.Version >= compareVersion)
            {
                await SaveSnapshotAsync(true);
            }
            return Snapshot.Version;
        }
        protected async ValueTask Tell(IFullyEvent<PrimaryKey> @event)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start event handling, grain Id = {0} and state version = {1},event type = {2} ,event = {3}", GrainId.ToString(), Snapshot.Version, @event.GetType().FullName, Serializer.SerializeToString(@event));
            try
            {
                if (@event.Base.Version == Snapshot.Version + 1)
                {
                    var onEventDeliveredTask = OnEventDelivered(@event);
                    if (!onEventDeliveredTask.IsCompletedSuccessfully)
                        await onEventDeliveredTask;
                    Snapshot.FullUpdateVersion(@event.Base, GrainType);//更新处理完成的Version
                }
                else if (@event.Base.Version > Snapshot.Version)
                {
                    var eventList = await EventStorage.GetList(GrainId, Snapshot.StartTimestamp, Snapshot.Version + 1, @event.Base.Version - 1);
                    foreach (var evt in eventList)
                    {
                        var onEventDeliveredTask = OnEventDelivered(evt);
                        if (!onEventDeliveredTask.IsCompletedSuccessfully)
                            await onEventDeliveredTask;
                        Snapshot.FullUpdateVersion(evt.Base, GrainType);//更新处理完成的Version
                    }
                }
                if (@event.Base.Version == Snapshot.Version + 1)
                {
                    var onEventDeliveredTask = OnEventDelivered(@event);
                    if (!onEventDeliveredTask.IsCompletedSuccessfully)
                        await onEventDeliveredTask;
                    Snapshot.FullUpdateVersion(@event.Base, GrainType);//更新处理完成的Version
                }
                if (@event.Base.Version > Snapshot.Version)
                {
                    throw new EventVersionUnorderedException(GrainId.ToString(), GrainType, @event.Base.Version, Snapshot.Version);
                }
                await SaveSnapshotAsync();
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Event Handling Completion, grain Id ={0} and state version = {1},event type = {2}", GrainId.ToString(), Snapshot.Version, @event.GetType().FullName);
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "FollowGrain Event handling failed with Id = {0},event = {1}", GrainId.ToString(), Serializer.SerializeToString(@event));
                throw;
            }
        }
        protected virtual ValueTask OnEventDelivered(IFullyEvent<PrimaryKey> @event)
        {
            if (SnapshotEventVersion > 0 && Snapshot.Version > 0 && @event.Base.Timestamp < Snapshot.StartTimestamp)
            {
                return new ValueTask(ObserverSnapshotStorage.UpdateStartTimestamp(Snapshot.StateId, @event.Base.Timestamp));
            }
            else
                return Consts.ValueTaskDone;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSaveSnapshot() => Consts.ValueTaskDone;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSavedSnapshot() => new ValueTask();
        protected virtual async ValueTask SaveSnapshotAsync(bool force = false)
        {
            if (SaveSnapshot)
            {
                if (force || (Snapshot.Version - SnapshotEventVersion >= ConfigOptions.ObserverSnapshotVersionInterval))
                {
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace("Start saving state snapshots with Id = {0} ,state version = {1}", GrainId.ToString(), Snapshot.Version);
                    try
                    {
                        var onSaveSnapshotTask = OnSaveSnapshot();//自定义保存项
                        if (!onSaveSnapshotTask.IsCompletedSuccessfully)
                            await onSaveSnapshotTask;
                        if (SnapshotEventVersion == 0)
                        {
                            await ObserverSnapshotStorage.Insert(Snapshot);
                        }
                        else
                        {
                            await ObserverSnapshotStorage.Update(Snapshot);
                        }
                        SnapshotEventVersion = Snapshot.Version;
                        var onSavedSnapshotTask = OnSavedSnapshot();
                        if (!onSavedSnapshotTask.IsCompletedSuccessfully)
                            await onSavedSnapshotTask;
                        if (Logger.IsEnabled(LogLevel.Trace))
                            Logger.LogTrace("State snapshot saved successfully with Id {0} ,state version = {1}", GrainId.ToString(), Snapshot.Version);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex, "State snapshot save failed with Id = {0}", GrainId.ToString());
                        throw;
                    }
                }
            }
        }
    }
}
