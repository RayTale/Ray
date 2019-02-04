using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.Abstractions;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.IGrains;
using Ray.Core.Logging;
using Ray.Core.Serialization;
using Ray.Core.State;
using Ray.Core.Storage;
using Ray.Core.Utils;

namespace Ray.Core
{
    public abstract class MainGrain<Children, PrimaryKey, State> : Grain
        where State : class, new()
    {
        public MainGrain(ILogger logger)
        {
            Logger = logger;
            GrainType = typeof(Children);
        }
        protected CoreOptions<Children> CoreOptions { get; private set; }
        protected ArchiveOptions<Children> ArchiveOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected IProducerContainer ProducerContainer { get; private set; }
        protected IStorageFactory StorageFactory { get; private set; }
        protected IJsonSerializer JsonSerializer { get; private set; }
        protected ISerializer Serializer { get; private set; }
        protected Snapshot<PrimaryKey, State> Snapshot { get; set; }
        protected IEventHandler<PrimaryKey, State> EventHandler { get; private set; }
        protected IFollowUnit<PrimaryKey> FollowUnit { get; private set; }
        /// <summary>
        /// 归档存储器
        /// </summary>
        protected IArchiveStorage<PrimaryKey, State> ArchiveStorage { get; private set; }
        protected List<ArchiveBrief> BriefArchiveList { get; private set; }
        protected ArchiveBrief LastArchive { get; private set; }
        protected ArchiveBrief NewArchive { get; private set; }
        protected ArchiveBrief ClearedArchive { get; private set; }
        public abstract PrimaryKey GrainId { get; }
        /// <summary>
        /// 快照的事件版本号
        /// </summary>
        protected long SnapshotEventVersion { get; private set; }
        /// <summary>
        /// 是否支持异步follow，true代表事件会广播，false事件不会进行广播
        /// </summary>
        protected virtual bool SupportFollow => true;
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
        protected ISnapshotStorage<PrimaryKey, State> SnapshotStorage { get; private set; }
        /// <summary>
        /// 事件发布器
        /// </summary>
        protected IProducer EventBusProducer { get; private set; }
        /// <summary>
        /// 依赖注入统一方法
        /// </summary>
        protected async virtual ValueTask DependencyInjection()
        {
            CoreOptions = ServiceProvider.GetService<IOptions<CoreOptions<Children>>>().Value;
            ArchiveOptions = ServiceProvider.GetService<IOptions<ArchiveOptions<Children>>>().Value;
            StorageFactory = ServiceProvider.GetService<IStorageFactoryContainer>().CreateFactory(GrainType);
            ProducerContainer = ServiceProvider.GetService<IProducerContainer>();
            Serializer = ServiceProvider.GetService<ISerializer>();
            JsonSerializer = ServiceProvider.GetService<IJsonSerializer>();
            EventHandler = ServiceProvider.GetService<IEventHandler<PrimaryKey, State>>();
            FollowUnit = ServiceProvider.GetService<IFollowUnitContainer>().GetUnit<PrimaryKey>(GrainType);
            //创建归档存储器
            if (ArchiveOptions.On)
            {
                var archiveStorageTask = StorageFactory.CreateArchiveStorage<PrimaryKey, State>(this, GrainId);
                if (!archiveStorageTask.IsCompletedSuccessfully)
                    await archiveStorageTask;
                ArchiveStorage = archiveStorageTask.Result;
            }
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
            //创建事件发布器
            var producerTask = ProducerContainer.GetProducer(this);
            if (!producerTask.IsCompletedSuccessfully)
                await producerTask;
            EventBusProducer = producerTask.Result;
        }
        /// <summary>
        /// Grain激活时调用用来初始化的方法(禁止在子类重写,请使用)
        /// </summary>
        /// <returns></returns>
        public override async Task OnActivateAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainActivateId, "Start activation grain with id = {0}", GrainId.ToString());
            var dITask = DependencyInjection();
            if (!dITask.IsCompletedSuccessfully)
                await dITask;
            try
            {
                if (ArchiveOptions.On)
                {
                    //加载归档信息
                    BriefArchiveList = (await ArchiveStorage.GetBriefList(GrainId)).OrderBy(a => a.Index).ToList();
                    LastArchive = BriefArchiveList.LastOrDefault();
                    ClearedArchive = BriefArchiveList.Where(a => a.EventIsCleared).OrderByDescending(a => a.Index).FirstOrDefault();
                    if (LastArchive != default && !LastArchive.IsCompletedArchive(ArchiveOptions) && !LastArchive.EventIsCleared)
                    {
                        await DeleteArchive(LastArchive.Id);
                        BriefArchiveList.Remove(LastArchive);
                        NewArchive = LastArchive;
                        LastArchive = BriefArchiveList.LastOrDefault();
                    }
                }
                //修复状态
                await RecoveryState();

                if (ArchiveOptions.On)
                {
                    if (Snapshot.Base.Version != 0 &&
                        (LastArchive == default || LastArchive.EndVersion < Snapshot.Base.Version) &&
                        (NewArchive == default || NewArchive.EndVersion < Snapshot.Base.Version))
                    {
                        //归档恢复
                        while (true)
                        {
                            var startTimestamp = Snapshot.Base.StartTimestamp;
                            long startVersion = 0;
                            if (NewArchive != default)
                            {
                                startVersion = NewArchive.EndVersion;
                                startTimestamp = NewArchive.StartTimestamp;
                            }
                            else if (NewArchive == default && LastArchive != default)
                            {
                                startVersion = LastArchive.EndVersion;
                                startTimestamp = LastArchive.EndTimestamp;
                            }
                            var eventList = await EventStorage.GetList(GrainId, startTimestamp, startVersion, startVersion + CoreOptions.NumberOfEventsPerRead);
                            foreach (var @event in eventList)
                            {
                                var task = EventArchive(@event);
                                if (!task.IsCompletedSuccessfully)
                                    await task;
                            }
                            if (eventList.Count < CoreOptions.NumberOfEventsPerRead) break;
                        };
                    }
                }
                var onActivatedTask = OnBaseActivated();
                if (!onActivatedTask.IsCompletedSuccessfully)
                    await onActivatedTask;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainActivateId, "Grain activation completed with id = {0}", GrainId.ToString());
            }
            catch (Exception ex)
            {
                Logger.LogCritical(LogEventIds.GrainActivateId, ex, "Grain activation failed with Id = {0}", GrainId.ToString());
                throw;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnBaseActivated() => Consts.ValueTaskDone;
        protected virtual async Task RecoveryState()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainStateRecoveryId, "The state of id = {0} begin to recover", GrainType.FullName, GrainId.ToString());
            try
            {
                await ReadSnapshotAsync();
                while (!Snapshot.Base.IsLatest)
                {
                    var eventList = await EventStorage.GetList(GrainId, Snapshot.Base.LatestMinEventTimestamp, Snapshot.Base.Version, Snapshot.Base.Version + CoreOptions.NumberOfEventsPerRead);
                    foreach (var @event in eventList)
                    {
                        Snapshot.Base.IncrementDoingVersion(GrainType);//标记将要处理的Version
                        EventApply(Snapshot, @event);
                        Snapshot.Base.UpdateVersion(@event.Base, GrainType);//更新处理完成的Version
                    }
                    if (eventList.Count < CoreOptions.NumberOfEventsPerRead) break;
                };
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainStateRecoveryId, "The state of id = {0} recovery has been completed ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
            }
            catch (Exception ex)
            {
                Logger.LogCritical(LogEventIds.GrainStateRecoveryId, ex, "The state of id = {0} recovery has failed ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
                throw;
            }
        }
        public override async Task OnDeactivateAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainDeactivateId, "Grain start deactivation with id = {0}", GrainId.ToString());
            var needSaveSnap = Snapshot.Base.Version - SnapshotEventVersion >= CoreOptions.MinSnapshotIntervalVersion;
            try
            {
                if (needSaveSnap)
                {
                    var saveTask = SaveSnapshotAsync(true, true);
                    if (!saveTask.IsCompletedSuccessfully)
                        await saveTask;
                    var onDeactivatedTask = OnDeactivated();
                    if (!onDeactivatedTask.IsCompletedSuccessfully)
                        await onDeactivatedTask;
                }
                if (ArchiveOptions.On && NewArchive != default)
                {
                    if (NewArchive.EndVersion - NewArchive.StartVersion >= ArchiveOptions.MinIntervalVersion)
                    {
                        var archiveTask = Archive(true);
                        if (!archiveTask.IsCompletedSuccessfully)
                            await archiveTask;
                    }
                }
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainDeactivateId, "Grain has been deactivated with id= {0} ,{1}", GrainId.ToString(), needSaveSnap ? "updated snapshot" : "no update snapshot");
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.GrainActivateId, ex, "Grain Deactivate failed with Id = {0}", GrainType.FullName, GrainId.ToString());
                throw;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnDeactivated() => Consts.ValueTaskDone;
        protected virtual async Task ReadSnapshotAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start read snapshot  with Id = {0} ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
            try
            {
                //从快照中恢复状态
                Snapshot = await SnapshotStorage.Get(GrainId);

                if (Snapshot == default)
                {
                    //从归档中恢复状态
                    if (ArchiveOptions.On && LastArchive != default)
                    {
                        Snapshot = await ArchiveStorage.GetState(LastArchive.Id);
                        await SaveSnapshotAsync(true, false);
                    }
                    if (Snapshot == default)
                    {
                        //新建状态
                        var createTask = CreateState();
                        if (!createTask.IsCompletedSuccessfully)
                            await createTask;
                    }
                }
                SnapshotEventVersion = Snapshot.Base.Version;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainSnapshot, "The snapshot of id = {0} read completed, state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Critical))
                    Logger.LogCritical(LogEventIds.GrainSnapshot, ex, "The snapshot of id = {0} read failed", GrainId.ToString());
                throw;
            }
        }

        protected async ValueTask SaveSnapshotAsync(bool force = false, bool isLatest = false)
        {
            if (Snapshot.Base.Version != Snapshot.Base.DoingVersion)
                throw new StateInsecurityException(Snapshot.Base.StateId.ToString(), GrainType, Snapshot.Base.DoingVersion, Snapshot.Base.Version);
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start save snapshot  with Id = {0} ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
            //如果版本号差超过设置则更新快照
            if (force || (Snapshot.Base.Version - SnapshotEventVersion >= CoreOptions.SnapshotIntervalVersion))
            {
                var oldLatestMinEventTimestamp = Snapshot.Base.LatestMinEventTimestamp;
                try
                {
                    var onSaveSnapshotTask = OnStartSaveSnapshot();
                    if (!onSaveSnapshotTask.IsCompletedSuccessfully)
                        await onSaveSnapshotTask;
                    Snapshot.Base.LatestMinEventTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    Snapshot.Base.IsLatest = isLatest;
                    if (SnapshotEventVersion == 0)
                    {
                        await SnapshotStorage.Insert(Snapshot);
                        SnapshotEventVersion = Snapshot.Base.Version;
                    }
                    else
                    {
                        await SnapshotStorage.Update(Snapshot);
                        SnapshotEventVersion = Snapshot.Base.Version;
                    }
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace(LogEventIds.GrainSnapshot, "The snapshot of id={0} save completed ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
                }
                catch (Exception ex)
                {
                    Snapshot.Base.LatestMinEventTimestamp = oldLatestMinEventTimestamp;
                    if (Logger.IsEnabled(LogLevel.Critical))
                        Logger.LogCritical(LogEventIds.GrainSnapshot, ex, "The snapshot of id= {0} save failed", GrainId.ToString());
                    throw;
                }
            }
        }
        protected async Task Over()
        {
            if (Snapshot.Base.IsOver)
                throw new StateIsOverException(Snapshot.Base.StateId.ToString(), GrainType);
            if (Snapshot.Base.Version != Snapshot.Base.DoingVersion)
                throw new StateInsecurityException(Snapshot.Base.StateId.ToString(), GrainType, Snapshot.Base.DoingVersion, Snapshot.Base.Version);
            if (CoreOptions.ClearEventWhenOver)
            {
                var versions = await Task.WhenAll(FollowUnit.GetAndSaveVersionFuncs().Select(func => func(Snapshot.Base.StateId, Snapshot.Base.Version)));
                if (versions.Any(v => v < Snapshot.Base.Version))
                {
                    throw new FollowNotCompletedException(GrainType.FullName, Snapshot.Base.StateId.ToString());
                }
            }
            Snapshot.Base.IsOver = true;
            Snapshot.Base.IsLatest = true;
            if (SnapshotEventVersion != Snapshot.Base.Version)
            {
                var saveTask = SaveSnapshotAsync(true, true);
                if (!saveTask.IsCompletedSuccessfully)
                    await saveTask;
            }
            else
            {
                await SnapshotStorage.Over(Snapshot.Base.StateId, true);
            }
            if (CoreOptions.ClearEventWhenOver)
            {
                await ArchiveStorage.DeleteAll(Snapshot.Base.StateId);
                await EventStorage.Delete(Snapshot.Base.StateId, Snapshot.Base.Version);
            }
            else
            {
                await ArchiveStorage.Over(Snapshot.Base.StateId, true);
            }
        }
        private async Task DeleteArchive(string briefId)
        {
            await ArchiveStorage.Delete(briefId);
            var task = OnStartDeleteArchive(briefId);
            if (!task.IsCompletedSuccessfully)
                await task;
        }
        private async Task DeleteAllArchive()
        {
            await DeleteAllArchive();
            var task = OnStartDeleteAllArchive();
            if (!task.IsCompletedSuccessfully)
                await task;
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
        protected virtual ValueTask CreateState()
        {
            Snapshot = new Snapshot<PrimaryKey, State>(GrainId);
            return Consts.ValueTaskDone;
        }
        /// <summary>
        /// 删除状态
        /// </summary>
        /// <returns></returns>
        protected async ValueTask DeleteState()
        {
            if (Snapshot.Base.IsOver)
                throw new StateIsOverException(Snapshot.Base.StateId.ToString(), GrainType);
            if (SnapshotEventVersion > 0)
            {
                await SnapshotStorage.Delete(GrainId);
                SnapshotEventVersion = 0;
            }
        }
        protected virtual async Task<bool> RaiseEvent(IEvent @event, EventUID uniqueId = null)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start raise event, grain Id ={0} and state version = {1},event type = {2} ,event ={3},uniqueueId= {4}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, JsonSerializer.Serialize(@event), uniqueId);
            if (Snapshot.Base.IsOver)
                throw new StateIsOverException(Snapshot.Base.StateId.ToString(), GrainType);
            try
            {
                var fullyEvent = new FullyEvent<PrimaryKey>
                {
                    Event = @event,
                    Base = new EventBase(),
                    StateId = Snapshot.Base.StateId
                };
                fullyEvent.Base.Version = Snapshot.Base.Version + 1;
                if (uniqueId == default) uniqueId = EventUID.Empty;
                if (string.IsNullOrEmpty(uniqueId.UID))
                    fullyEvent.Base.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                else
                    fullyEvent.Base.Timestamp = uniqueId.Timestamp;
                var startTask = OnRaiseStart(fullyEvent);
                if (!startTask.IsCompletedSuccessfully)
                    await startTask;
                Snapshot.Base.IncrementDoingVersion(GrainType);//标记将要处理的Version
                using (var ms = new PooledMemoryStream())
                {
                    Serializer.Serialize(ms, @event);
                    var bytesTransport = new BytesTransport
                    {
                        EventType = @event.GetType().FullName,
                        ActorId = Snapshot.Base.StateId,
                        EventBytes = ms.ToArray(),
                        BaseBytes = fullyEvent.Base.GetBytes()
                    };
                    if (await EventStorage.Append(new SaveTransport<PrimaryKey>(fullyEvent, bytesTransport, uniqueId.UID)))
                    {
                        if (SupportFollow)
                        {
                            //消息写入消息队列，以提供异步服务
                            EventApply(Snapshot, fullyEvent);
                            try
                            {
                                var publishTask = EventBusProducer.Publish(bytesTransport.GetBytes(), GrainId.ToString());
                                if (!publishTask.IsCompletedSuccessfully)
                                    await publishTask;
                            }
                            catch (Exception ex)
                            {
                                if (Logger.IsEnabled(LogLevel.Error))
                                    Logger.LogError(LogEventIds.GrainRaiseEvent, ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), Snapshot.Base.Version);
                            }
                        }
                        else
                        {
                            EventApply(Snapshot, fullyEvent);
                        }
                        Snapshot.Base.UpdateVersion(fullyEvent.Base, GrainType);//更新处理完成的Version
                        var saveSnapshotTask = SaveSnapshotAsync();
                        if (!saveSnapshotTask.IsCompletedSuccessfully)
                            await saveSnapshotTask;
                        var task = OnRaiseSuccessed(fullyEvent, bytesTransport);
                        if (!task.IsCompletedSuccessfully)
                            await task;
                        if (Logger.IsEnabled(LogLevel.Trace))
                            Logger.LogTrace(LogEventIds.GrainRaiseEvent, "Raise event successfully, grain Id= {0} and state version = {1}}", GrainId.ToString(), Snapshot.Base.Version);
                        return true;
                    }
                    else
                    {
                        if (Logger.IsEnabled(LogLevel.Information))
                            Logger.LogInformation(LogEventIds.GrainRaiseEvent, "Raise event failure because of idempotency limitation, grain Id = {0},state version = {1},event type = {2} with version = {3}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, fullyEvent.Base.Version);
                        var task = OnRaiseFailed(fullyEvent);
                        if (!task.IsCompletedSuccessfully)
                            await task;
                        Snapshot.Base.DecrementDoingVersion();//还原doing Version
                    }
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.GrainRaiseEvent, ex, "Raise event produces errors, state Id = {0}, version ={1},event type = {2},event = {3}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, JsonSerializer.Serialize(@event));
                await RecoveryState();//还原状态
                throw;
            }
            return false;
        }

        protected virtual async ValueTask OnRaiseStart(IFullyEvent<PrimaryKey> @event)
        {
            if (Snapshot.Base.Version == 0)
                return;
            if (Snapshot.Base.IsLatest)
            {
                await SnapshotStorage.UpdateIsLatest(Snapshot.Base.StateId, false);
                Snapshot.Base.IsLatest = false;
            }
            if (@event.Base.Timestamp < ClearedArchive.StartTimestamp)
            {
                throw new EventIsClearedException(@event.GetType().FullName, JsonSerializer.Serialize(@event), ClearedArchive.Index);
            }
            if (SnapshotEventVersion > 0)
            {
                if (@event.Base.Timestamp < Snapshot.Base.LatestMinEventTimestamp)
                {
                    await SnapshotStorage.UpdateLatestMinEventTimestamp(Snapshot.Base.StateId, @event.Base.Timestamp);
                }
                if (@event.Base.Timestamp < Snapshot.Base.StartTimestamp)
                {
                    await SnapshotStorage.UpdateStartTimestamp(Snapshot.Base.StateId, @event.Base.Timestamp);
                }
            }
            if (ArchiveOptions.On &&
                LastArchive != default &&
                @event.Base.Timestamp < LastArchive.EndTimestamp)
            {
                foreach (var archive in BriefArchiveList.Where(a => @event.Base.Timestamp < a.EndTimestamp && !a.EventIsCleared).OrderByDescending(v => v.Index))
                {
                    if (@event.Base.Timestamp < archive.EndTimestamp)
                    {
                        await DeleteArchive(archive.Id);
                        if (NewArchive != default)
                            NewArchive = CombineArchiveInfo(archive, NewArchive);
                        else
                            NewArchive = archive;
                        BriefArchiveList.Remove(archive);
                    }
                }
                LastArchive = BriefArchiveList.LastOrDefault();
            }
        }
        protected virtual ValueTask OnRaiseSuccessed(IFullyEvent<PrimaryKey> @event, BytesTransport bytesTransport)
        {
            if (ArchiveOptions.On)
            {
                return EventArchive(@event);
            }
            return Consts.ValueTaskDone;
        }
        protected virtual ValueTask OnRaiseFailed(IFullyEvent<PrimaryKey> @event)
        {
            if (ArchiveOptions.On && NewArchive != default)
            {
                return Archive();
            }
            return Consts.ValueTaskDone;
        }
        protected virtual void EventApply(Snapshot<PrimaryKey, State> snapshot, IFullyEvent<PrimaryKey> evt)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainRaiseEvent, "Start apply event, grain Id= {0} and state version is {1}},event type = {2},event = {3}", GrainId.ToString(), Snapshot.Base.Version, evt.GetType().FullName, JsonSerializer.Serialize(evt));
            EventHandler.Apply(snapshot, evt);
        }
        protected async ValueTask EventArchive(IFullyEvent<PrimaryKey> @event)
        {
            if (NewArchive == default)
            {
                NewArchive = new ArchiveBrief
                {
                    Id = await ServiceProvider.GetService<IGrainFactory>().GetGrain<IUID>(GrainType.FullName).NewUtcID(),
                    StartTimestamp = @event.Base.Timestamp,
                    StartVersion = @event.Base.Version,
                    Index = LastArchive != default ? LastArchive.Index + 1 : 0,
                    EndTimestamp = @event.Base.Timestamp,
                    EndVersion = @event.Base.Version
                };
            }
            else
            {
                //判定有没有时间戳小于前一个归档
                if (NewArchive.StartTimestamp == 0 || @event.Base.Timestamp < NewArchive.StartTimestamp)
                    NewArchive.StartTimestamp = @event.Base.Timestamp;
                if (@event.Base.Timestamp > NewArchive.StartTimestamp)
                    NewArchive.EndTimestamp = @event.Base.Timestamp;
                NewArchive.EndVersion = @event.Base.Version;
            }
            var archiveTask = Archive();
            if (!archiveTask.IsCompletedSuccessfully)
                await archiveTask;
        }
        private ArchiveBrief CombineArchiveInfo(ArchiveBrief main, ArchiveBrief merge)
        {
            if (merge.StartTimestamp < main.StartTimestamp)
                main.StartTimestamp = merge.StartTimestamp;
            if (merge.StartVersion < main.StartVersion)
                main.StartVersion = merge.StartVersion;
            if (merge.EndTimestamp > main.EndTimestamp)
                main.EndTimestamp = merge.EndTimestamp;
            if (merge.EndVersion > main.EndVersion)
                main.EndVersion = merge.EndVersion;
            return main;
        }

        protected async ValueTask Archive(bool force = false)
        {
            if (Snapshot.Base.Version != Snapshot.Base.DoingVersion)
                throw new StateInsecurityException(Snapshot.Base.StateId.ToString(), GrainType, Snapshot.Base.DoingVersion, Snapshot.Base.Version);
            var intervalMilliseconds = LastArchive == default ? 0 : NewArchive.EndTimestamp - LastArchive.EndTimestamp;
            var intervalVersiion = NewArchive.EndVersion - NewArchive.StartVersion;
            if (force || (
                (intervalMilliseconds > ArchiveOptions.IntervalMilliSeconds && intervalVersiion > ArchiveOptions.IntervalVersion) ||
                intervalMilliseconds > ArchiveOptions.MaxIntervalMilliSeconds ||
                intervalVersiion > ArchiveOptions.MaxIntervalVersion
                ))
            {
                var task = OnStartArchive();
                if (!task.IsCompletedSuccessfully)
                    await task;
                await ArchiveStorage.Insert(NewArchive, Snapshot);
                BriefArchiveList.Add(NewArchive);
                LastArchive = NewArchive;
                NewArchive = default;
                var onTask = OnArchiveCompleted();
                if (!onTask.IsCompletedSuccessfully)
                    await onTask;
            }
        }
        protected virtual async ValueTask OnArchiveCompleted()
        {
            //开始执行事件清理逻辑
            var noCleareds = BriefArchiveList.Where(a => !a.EventIsCleared).ToList();
            if (noCleareds.Count >= ArchiveOptions.EventClearIntervalArchive)
            {
                var minArchive = noCleareds.FirstOrDefault();
                if (minArchive != default)
                {
                    //判断需要清理的event是否都被follow执行过
                    var versions = await Task.WhenAll(FollowUnit.GetAndSaveVersionFuncs().Select(func => func(Snapshot.Base.StateId, Snapshot.Base.Version)));
                    if (versions.All(v => v >= minArchive.EndVersion))
                    {
                        //清理归档对应的事件
                        await ArchiveStorage.EventIsClear(minArchive.Id);
                        minArchive.EventIsCleared = true;
                        //如果快照的版本小于需要清理的最大事件版本号，则保存快照
                        if (SnapshotEventVersion < minArchive.EndVersion)
                        {
                            var saveTask = SaveSnapshotAsync(true);
                            if (!saveTask.IsCompletedSuccessfully)
                                await saveTask;
                        }
                        await EventStorage.Delete(Snapshot.Base.StateId, minArchive.EndVersion);
                        ClearedArchive = minArchive;
                        //只保留一个清理过事件的快照，其它的删除掉
                        var cleareds = BriefArchiveList.Where(a => a.EventIsCleared).OrderBy(a => a.Index).ToArray();
                        if (cleareds.Length > 1)
                        {
                            for (int i = 0; i < cleareds.Length - 1; i++)
                            {
                                await DeleteArchive(cleareds[i].Id);
                                BriefArchiveList.Remove(cleareds[i]);
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
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.MessagePublish, "Start publishing, grain Id= {0}, message type = {1},message = {2},hashkey={3}", GrainId.ToString(), msg.GetType().FullName, JsonSerializer.Serialize(msg), hashKey);
            if (string.IsNullOrEmpty(hashKey))
                hashKey = GrainId.ToString();
            try
            {
                using (var ms = new PooledMemoryStream())
                {
                    Serializer.Serialize(ms, msg);
                    var wrapper = new BytesWrapper(msg.GetType().FullName, ms.ToArray());
                    var pubLishTask = EventBusProducer.Publish(wrapper.GetBytes(), hashKey);
                    if (!pubLishTask.IsCompletedSuccessfully)
                        await pubLishTask;
                }

            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.MessagePublish, ex, "Publish message errors, grain Id= {0}, message type = {1},message = {2},hashkey={3}", GrainId.ToString(), msg.GetType().FullName, JsonSerializer.Serialize(msg), hashKey);
                throw;
            }
        }
    }
}
