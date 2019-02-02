using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Logging;
using Ray.Core.Serialization;
using Ray.Core.State;
using Ray.Core.Storage;

namespace Ray.Core
{
    public abstract class ReplicaGrain<Main, PrimaryKey, State> : Grain, IFollow
        where State : class, new()
    {
        public ReplicaGrain(ILogger logger)
        {
            Logger = logger;
            GrainType = GetType();
        }
        protected CoreOptions<Main> CoreOptions { get; private set; }
        protected ArchiveOptions<Main> ArchiveOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected IJsonSerializer JsonSerializer { get; private set; }
        protected ISerializer Serializer { get; private set; }
        protected IStorageFactory StorageFactory { get; private set; }
        protected Snapshot<PrimaryKey, State> Snapshot { get; set; }
        public abstract PrimaryKey GrainId { get; }
        /// <summary>
        /// 分批次批量读取事件的时候每次读取的数据量
        /// </summary>
        protected virtual int NumberOfEventsPerRead => CoreOptions.NumberOfEventsPerRead;
        /// <summary>
        /// 事件处理的超时时间
        /// </summary>
        protected virtual int EventAsyncProcessTimeoutSeconds => CoreOptions.EventAsyncProcessTimeoutSeconds;
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
        protected ISnapshotStorage<PrimaryKey, State> SnapshotStorage { get; private set; }
        /// <summary>
        /// 归档存储器
        /// </summary>
        protected IArchiveStorage<PrimaryKey, State> ArchiveStorage { get; private set; }
        protected IEventHandler<PrimaryKey, State> EventHandler { get; private set; }
        protected ArchiveBrief LastArchive { get; private set; }
        #region 初始化数据
        /// <summary>
        /// 依赖注入统一方法
        /// </summary>
        protected async virtual ValueTask DependencyInjection()
        {
            CoreOptions = ServiceProvider.GetService<IOptions<CoreOptions<Main>>>().Value;
            ArchiveOptions = ServiceProvider.GetService<IOptions<ArchiveOptions<Main>>>().Value;
            StorageFactory = ServiceProvider.GetService<IStorageFactoryContainer>().CreateFactory(GrainType);
            Serializer = ServiceProvider.GetService<ISerializer>();
            JsonSerializer = ServiceProvider.GetService<IJsonSerializer>();
            EventHandler = ServiceProvider.GetService<IEventHandler<PrimaryKey, State>>();
            //创建归档存储器
            var archiveStorageTask = StorageFactory.CreateArchiveStorage<PrimaryKey, State>(this, GrainId);
            if (!archiveStorageTask.IsCompletedSuccessfully)
                await archiveStorageTask;
            ArchiveStorage = archiveStorageTask.Result;
            //创建事件存储器
            var eventStorageTask = StorageFactory.CreateEventStorage(this, GrainId);
            if (!eventStorageTask.IsCompletedSuccessfully)
                await eventStorageTask;
            EventStorage = eventStorageTask.Result;
            //创建状态存储器
            var stateStorageTask = StorageFactory.CreateSnapshotStorage<PrimaryKey, State>(this, GrainId);
            if (!stateStorageTask.IsCompletedSuccessfully)
                await stateStorageTask;
            SnapshotStorage = stateStorageTask.Result;
        }
        public override async Task OnActivateAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainActivateId, "Start activation followgrain with id = {0}", GrainId.ToString());
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
                    await FullActive();
                }
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainActivateId, "Followgrain activation completed with id = {0}", GrainId.ToString());
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(LogEventIds.FollowGrainActivateId, ex, "Followgrain activation failed with Id = {0}", GrainId.ToString());
                throw;
            }
        }
        private async Task FullActive()
        {
            while (true)
            {
                var eventList = await EventStorage.GetList(GrainId, 0, Snapshot.Base.Version, Snapshot.Base.Version + NumberOfEventsPerRead);
                foreach (var @event in eventList)
                {
                    Snapshot.Base.IncrementDoingVersion(GrainType);//标记将要处理的Version
                    var task = OnEventDelivered(@event);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    Snapshot.Base.UpdateVersion(@event.Base, GrainType);//更新处理完成的Version
                }
                if (eventList.Count < NumberOfEventsPerRead) break;
            };
        }
        protected virtual async Task ReadSnapshotAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start read snapshot  with Id = {0} ,state version = {1}", GrainId.ToString(), this.Snapshot.Base.Version);
            try
            {
                Snapshot = await SnapshotStorage.Get(GrainId);
                if (Snapshot == default)
                {
                    //从归档中恢复状态
                    if (ArchiveOptions.On && LastArchive != default)
                    {
                        Snapshot = await ArchiveStorage.GetState(LastArchive.Id);
                    }
                }
                if (Snapshot == default)
                {
                    //新建状态
                    var createTask = CreateState();
                    if (!createTask.IsCompletedSuccessfully)
                        await createTask;
                }
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainSnapshot, "The snapshot of id = {0} read completed, state version = {1}", GrainId.ToString(), this.Snapshot.Base.Version);
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(LogEventIds.GrainSnapshot, ex, "The snapshot of id = {0} read failed", GrainId.ToString());
                throw;
            }
        }
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask CreateState()
        {
            Snapshot = new Snapshot<PrimaryKey, State>(GrainId);
            return Consts.ValueTaskDone;
        }
        #endregion
        public Task Tell(byte[] bytes)
        {
            var (success, transport) = BytesTransport.FromBytesWithNoId(bytes);
            if (success)
            {
                var eventType = TypeContainer.GetType(transport.EventType);
                using (var ms = new MemoryStream(transport.EventBytes))
                {
                    var data = Serializer.Deserialize(eventType, ms);
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
                            Logger.LogInformation(LogEventIds.FollowEventProcessing, "Receive non-event messages, grain Id = {0} ,message type = {1}", GrainId.ToString(), transport.EventType);
                    }
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
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.FollowEventProcessing, "Start event handling, grain Id = {0} and state version = {1},event type = {2} ,event = {3}", GrainId.ToString(), this.Snapshot.Base.Version, @event.GetType().FullName, JsonSerializer.Serialize(@event));
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
                    var eventList = await EventStorage.GetList(GrainId, 0, Snapshot.Base.Version, @event.Base.Version);
                    foreach (var evt in eventList)
                    {
                        var onEventDeliveredTask = OnEventDelivered(evt);
                        if (!onEventDeliveredTask.IsCompletedSuccessfully)
                            await onEventDeliveredTask;
                        Snapshot.Base.FullUpdateVersion(@event.Base, GrainType);//更新处理完成的Version
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
                    throw new EventVersionNotMatchStateException(GrainId.ToString(), GrainType, @event.Base.Version, this.Snapshot.Base.Version);
                }
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.FollowEventProcessing, "Event Handling Completion, grain Id ={0} and state version = {1},event type = {2}", GrainId.ToString(), this.Snapshot.Base.Version, @event.GetType().FullName);
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(LogEventIds.FollowEventProcessing, ex, "FollowGrain Event handling failed with Id = {0},event = {1}", GrainId.ToString(), JsonSerializer.Serialize(@event));
                throw;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnEventDelivered(IFullyEvent<PrimaryKey> @event)
        {
            EventHandler.Apply(Snapshot, @event);
            return Consts.ValueTaskDone;
        }
    }
}
