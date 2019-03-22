using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Abstractions;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.EventBus;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Services.Abstractions;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Core
{
    public abstract class RayGrain<Grain, PrimaryKey, StateType> : Orleans.Grain
        where StateType : class, new()
    {
        public RayGrain()
        {
            GrainType = typeof(Grain);
        }
        protected CoreOptions CoreOptions { get; private set; }
        protected ArchiveOptions ArchiveOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected IProducerContainer ProducerContainer { get; private set; }
        protected ISerializer Serializer { get; private set; }
        protected Snapshot<PrimaryKey, StateType> Snapshot { get; set; }
        protected IEventHandler<PrimaryKey, StateType> EventHandler { get; private set; }
        protected IObserverUnit<PrimaryKey> FollowUnit { get; private set; }
        /// <summary>
        /// 归档存储器
        /// </summary>
        protected IArchiveStorage<PrimaryKey, StateType> ArchiveStorage { get; private set; }
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
        /// 依赖注入统一方法
        /// </summary>
        protected async virtual ValueTask DependencyInjection()
        {
            CoreOptions = ServiceProvider.GetOptionsByName<CoreOptions>(GrainType.FullName);
            ArchiveOptions = ServiceProvider.GetOptionsByName<ArchiveOptions>(GrainType.FullName);
            Logger = ServiceProvider.GetService<ILogger<Grain>>();
            ProducerContainer = ServiceProvider.GetService<IProducerContainer>();
            Serializer = ServiceProvider.GetService<ISerializer>();
            EventHandler = ServiceProvider.GetService<IEventHandler<PrimaryKey, StateType>>();
            FollowUnit = ServiceProvider.GetService<IObserverUnitContainer>().GetUnit<PrimaryKey>(GrainType);
            var configureBuilder = ServiceProvider.GetService<IConfigureBuilder<PrimaryKey, Grain>>();
            var storageConfigTask = configureBuilder.GetConfig(ServiceProvider, GrainId);
            if (!storageConfigTask.IsCompletedSuccessfully)
                await storageConfigTask;
            var storageFactory = ServiceProvider.GetService(configureBuilder.StorageFactory) as IStorageFactory;
            //创建归档存储器
            if (ArchiveOptions.On)
            {
                var archiveStorageTask = storageFactory.CreateArchiveStorage<PrimaryKey, StateType>(storageConfigTask.Result, GrainId);
                if (!archiveStorageTask.IsCompletedSuccessfully)
                    await archiveStorageTask;
                ArchiveStorage = archiveStorageTask.Result;
            }
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
            var dITask = DependencyInjection();
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start activation with id = {0}", GrainId.ToString());
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
                    var secondLastArchive = BriefArchiveList.Count > 1 ? BriefArchiveList.SkipLast(1).Last() : default;
                    if (LastArchive != default && !LastArchive.IsCompletedArchive(ArchiveOptions, secondLastArchive) && !LastArchive.EventIsCleared)
                    {
                        await DeleteArchive(LastArchive.Id);
                        BriefArchiveList.Remove(LastArchive);
                        NewArchive = LastArchive;
                        LastArchive = BriefArchiveList.LastOrDefault();
                    }
                }
                //修复状态
                await RecoverySnapshot();

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
                            var eventList = await EventStorage.GetList(GrainId, startTimestamp, startVersion + 1, startVersion + CoreOptions.NumberOfEventsPerRead);
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
                    Logger.LogTrace("Activation completed with id = {0}", GrainId.ToString());
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Activation failed with Id = {0}", GrainId.ToString());
                throw;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnBaseActivated() => Consts.ValueTaskDone;
        protected virtual async Task RecoverySnapshot()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("The state of id = {0} begin to recover", GrainType.FullName, GrainId.ToString());
            try
            {
                await ReadSnapshotAsync();
                while (!Snapshot.Base.IsLatest)
                {
                    var eventList = await EventStorage.GetList(GrainId, Snapshot.Base.LatestMinEventTimestamp, Snapshot.Base.Version + 1, Snapshot.Base.Version + CoreOptions.NumberOfEventsPerRead);
                    foreach (var fullyEvent in eventList)
                    {
                        Snapshot.Base.IncrementDoingVersion(GrainType);//标记将要处理的Version
                        EventHandler.Apply(Snapshot, fullyEvent);
                        Snapshot.Base.UpdateVersion(fullyEvent.Base, GrainType);//更新处理完成的Version
                    }
                    if (eventList.Count < CoreOptions.NumberOfEventsPerRead) break;
                };
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("The state of id = {0} recovery has been completed ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "The state of id = {0} recovery has failed ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
                throw;
            }
        }
        public override async Task OnDeactivateAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Grain start deactivation with id = {0}", GrainId.ToString());
            var needSaveSnap = Snapshot.Base.Version - SnapshotEventVersion >= CoreOptions.MinSnapshotVersionInterval;
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
                    if (NewArchive.EndVersion - NewArchive.StartVersion >= ArchiveOptions.MinVersionIntervalAtDeactivate)
                    {
                        var archiveTask = Archive(true);
                        if (!archiveTask.IsCompletedSuccessfully)
                            await archiveTask;
                    }
                }
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Grain has been deactivated with id= {0} ,{1}", GrainId.ToString(), needSaveSnap ? "updated snapshot" : "no update snapshot");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Grain Deactivate failed with Id = {0}", GrainId.ToString());
                throw;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnDeactivated() => Consts.ValueTaskDone;
        protected virtual async Task ReadSnapshotAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start read snapshot  with Id = {0} ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
            try
            {
                //从快照中恢复状态
                Snapshot = await SnapshotStorage.Get(GrainId);

                if (Snapshot == default)
                {
                    //从归档中恢复状态
                    if (ArchiveOptions.On && LastArchive != default)
                    {
                        Snapshot = await ArchiveStorage.GetById(LastArchive.Id);
                        await SaveSnapshotAsync(true, false);
                    }
                    if (Snapshot == default)
                    {
                        //新建状态
                        var createTask = CreateSnapshot();
                        if (!createTask.IsCompletedSuccessfully)
                            await createTask;
                    }
                }
                SnapshotEventVersion = Snapshot.Base.Version;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("The snapshot of id = {0} read completed, state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "The snapshot of id = {0} read failed", GrainId.ToString());
                throw;
            }
        }

        protected async ValueTask SaveSnapshotAsync(bool force = false, bool isLatest = false)
        {
            if (Snapshot.Base.Version != Snapshot.Base.DoingVersion)
                throw new StateInsecurityException(Snapshot.Base.StateId.ToString(), GrainType, Snapshot.Base.DoingVersion, Snapshot.Base.Version);
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Start save snapshot  with Id = {0} ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
            //如果版本号差超过设置则更新快照
            if (force || (Snapshot.Base.Version - SnapshotEventVersion >= CoreOptions.SnapshotVersionInterval))
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
                    }
                    else
                    {
                        await SnapshotStorage.Update(Snapshot);
                    }
                    SnapshotEventVersion = Snapshot.Base.Version;
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace("The snapshot of id={0} save completed ,state version = {1}", GrainId.ToString(), Snapshot.Base.Version);
                }
                catch (Exception ex)
                {
                    Snapshot.Base.LatestMinEventTimestamp = oldLatestMinEventTimestamp;
                    Logger.LogCritical(ex, "The snapshot of id= {0} save failed", GrainId.ToString());
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
            if (ArchiveOptions.On && ArchiveOptions.ArchiveEventOnOver)
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
            if (ArchiveOptions.On && ArchiveOptions.ArchiveEventOnOver)
            {
                await ArchiveStorage.DeleteAll(Snapshot.Base.StateId);
                if (ArchiveOptions.EventArchiveType == EventArchiveType.Delete)
                    await EventStorage.DeleteStart(Snapshot.Base.StateId, Snapshot.Base.Version, Snapshot.Base.StartTimestamp);
                else
                    await ArchiveStorage.EventArichive(Snapshot.Base.StateId, Snapshot.Base.Version, Snapshot.Base.StartTimestamp);
            }
            else
            {
                await ArchiveStorage.Over(Snapshot.Base.StateId, true);
            }
        }
        private async Task DeleteArchive(string briefId)
        {
            await ArchiveStorage.Delete(GrainId, briefId);
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
        protected virtual ValueTask CreateSnapshot()
        {
            Snapshot = new Snapshot<PrimaryKey, StateType>(GrainId);
            return Consts.ValueTaskDone;
        }
        /// <summary>
        /// 删除状态
        /// </summary>
        /// <returns></returns>
        protected async ValueTask DeleteSnapshot()
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
                Logger.LogTrace("Start raise event, grain Id ={0} and state version = {1},event type = {2} ,event ={3},uniqueueId= {4}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, Serializer.SerializeToString(@event), uniqueId);
            if (Snapshot.Base.IsOver)
                throw new StateIsOverException(Snapshot.Base.StateId.ToString(), GrainType);
            try
            {
                var fullyEvent = new FullyEvent<PrimaryKey>
                {
                    Event = @event,
                    Base = new EventBase
                    {
                        Version = Snapshot.Base.Version + 1
                    },
                    StateId = Snapshot.Base.StateId
                };
                if (uniqueId == default) uniqueId = EventUID.Empty;
                if (string.IsNullOrEmpty(uniqueId.UID))
                    fullyEvent.Base.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                else
                    fullyEvent.Base.Timestamp = uniqueId.Timestamp;
                var startTask = OnRaiseStart(fullyEvent);
                if (!startTask.IsCompletedSuccessfully)
                    await startTask;
                Snapshot.Base.IncrementDoingVersion(GrainType);//标记将要处理的Version
                var bytesTransport = new EventBytesTransport(
                    @event.GetType().FullName,
                    Snapshot.Base.StateId,
                    fullyEvent.Base.GetBytes(),
                    Serializer.SerializeToBytes(@event)
                );
                if (await EventStorage.Append(fullyEvent, in bytesTransport, uniqueId.UID))
                {
                    EventHandler.Apply(Snapshot, fullyEvent);
                    Snapshot.Base.UpdateVersion(fullyEvent.Base, GrainType);//更新处理完成的Version
                    var task = OnRaiseSuccessed(fullyEvent, bytesTransport);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    var saveSnapshotTask = SaveSnapshotAsync();
                    if (!saveSnapshotTask.IsCompletedSuccessfully)
                        await saveSnapshotTask;
                    await PublishToEventBust(bytesTransport);
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace("Raise event successfully, grain Id= {0} and state version = {1}}", GrainId.ToString(), Snapshot.Base.Version);
                    return true;
                }
                else
                {
                    if (Logger.IsEnabled(LogLevel.Information))
                        Logger.LogInformation("Raise event failure because of idempotency limitation, grain Id = {0},state version = {1},event type = {2} with version = {3}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, fullyEvent.Base.Version);
                    var task = OnRaiseFailed(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    Snapshot.Base.DecrementDoingVersion();//还原doing Version
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(ex, "Raise event produces errors, state Id = {0}, version ={1},event type = {2},event = {3}", GrainId.ToString(), Snapshot.Base.Version, @event.GetType().FullName, Serializer.SerializeToString(@event));
                await RecoverySnapshot();//还原状态
                throw;
            }
            return false;
            //发送事件到EventBus中
            async Task PublishToEventBust(EventBytesTransport bytesTransport)
            {
                var handlers = FollowUnit.GetAllEventHandlers();
                if (handlers.Count > 0)
                {
                    try
                    {
                        if (CoreOptions.PriorityAsyncEventBus)
                        {
                            try
                            {
                                var publishTask = EventBusProducer.Publish(bytesTransport.GetBytes(), GrainId.ToString());
                                if (!publishTask.IsCompletedSuccessfully)
                                    await publishTask;
                            }
                            catch (Exception ex)
                            {
                                Logger.LogError(ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), Snapshot.Base.Version);
                                //当消息队列出现问题的时候同步推送
                                await Task.WhenAll(handlers.Select(func => func(bytesTransport.GetBytes())));
                            }
                        }
                        else
                        {
                            try
                            {
                                await Task.WhenAll(handlers.Select(func => func(bytesTransport.GetBytes())));
                            }
                            catch (Exception ex)
                            {
                                Logger.LogError(ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), Snapshot.Base.Version);
                                //当消息队列出现问题的时候异步推送
                                var publishTask = EventBusProducer.Publish(bytesTransport.GetBytes(), GrainId.ToString());
                                if (!publishTask.IsCompletedSuccessfully)
                                    await publishTask;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), Snapshot.Base.Version);
                    }
                }
            }
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
            if (ClearedArchive != default && @event.Base.Timestamp < ClearedArchive.StartTimestamp)
            {
                throw new EventIsClearedException(@event.GetType().FullName, Serializer.SerializeToString(@event), ClearedArchive.Index);
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
        protected virtual ValueTask OnRaiseSuccessed(IFullyEvent<PrimaryKey> @event, EventBytesTransport bytesTransport)
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
            if (force || NewArchive.IsCompletedArchive(ArchiveOptions, LastArchive))
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
            if (noCleareds.Count >= ArchiveOptions.MaxSnapshotArchiveRecords)
            {
                var minArchive = noCleareds.FirstOrDefault();
                if (minArchive != default)
                {
                    //判断需要清理的event是否都被follow执行过
                    var versions = await Task.WhenAll(FollowUnit.GetAndSaveVersionFuncs().Select(func => func(Snapshot.Base.StateId, Snapshot.Base.Version)));
                    if (versions.All(v => v >= minArchive.EndVersion))
                    {
                        //清理归档对应的事件
                        await ArchiveStorage.EventIsClear(Snapshot.Base.StateId, minArchive.Id);
                        minArchive.EventIsCleared = true;
                        //如果快照的版本小于需要清理的最大事件版本号，则保存快照
                        if (SnapshotEventVersion < minArchive.EndVersion)
                        {
                            var saveTask = SaveSnapshotAsync(true);
                            if (!saveTask.IsCompletedSuccessfully)
                                await saveTask;
                        }
                        if (ArchiveOptions.EventArchiveType == EventArchiveType.Delete)
                            await EventStorage.DeleteStart(Snapshot.Base.StateId, minArchive.EndVersion, Snapshot.Base.StartTimestamp);
                        else
                            await ArchiveStorage.EventArichive(Snapshot.Base.StateId, minArchive.EndVersion, Snapshot.Base.StartTimestamp);
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
                Logger.LogTrace("Start publishing, grain Id= {0}, message type = {1},message = {2},hashkey={3}", GrainId.ToString(), msg.GetType().FullName, Serializer.SerializeToString(msg), hashKey);
            if (string.IsNullOrEmpty(hashKey))
                hashKey = GrainId.ToString();
            try
            {
                var wrapper = new CommonTransport(msg.GetType().FullName, Serializer.SerializeToBytes(msg));
                var pubLishTask = EventBusProducer.Publish(wrapper.GetBytes(), hashKey);
                if (!pubLishTask.IsCompletedSuccessfully)
                    await pubLishTask;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Publish message errors, grain Id= {0}, message type = {1},message = {2},hashkey={3}", GrainId.ToString(), msg.GetType().FullName, Serializer.SerializeToString(msg), hashKey);
                throw;
            }
        }
    }
}
