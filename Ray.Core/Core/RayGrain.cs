using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
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
    public abstract class RayGrain<K, E, S, B, W> : Grain
        where E : IEventBase<K>
        where S : class, IState<K, B>, new()
        where B : ISnapshot<K>, new()
        where W : IBytesWrapper, new()
    {
        public RayGrain(ILogger logger)
        {
            Logger = logger;
            GrainType = GetType();
        }
        protected BaseOptions BaseOptions { get; private set; }
        protected OverOptions OverOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected IProducerContainer ProducerContainer { get; private set; }
        protected IStorageFactory StorageFactory { get; private set; }
        protected IJsonSerializer JsonSerializer { get; private set; }
        protected ISerializer Serializer { get; private set; }
        protected S State { get; set; }
        protected IEventHandler<K, E, S, B> EventHandler { get; private set; }
        protected IFollowUnit<K> FollowUnit { get; private set; }
        /// <summary>
        /// 归档存储器
        /// </summary>
        protected IArchiveStorage<K, S, B> ArchiveStorage { get; private set; }
        protected ArchiveOptions ArchiveOptions { get; private set; }
        protected List<ArchiveBrief> BriefArchiveList { get; private set; }
        protected ArchiveBrief LastArchive { get; private set; }
        protected ArchiveBrief NewArchive { get; private set; }
        public abstract K GrainId { get; }
        /// <summary>
        /// 快照
        /// </summary>
        protected virtual SnapshotProcessor SnapshotProcessor => SnapshotProcessor.Master;
        /// <summary>
        /// 保存快照的事件Version间隔
        /// </summary>
        protected virtual int SnapshotIntervalVersion => BaseOptions.SnapshotIntervalVersion;
        /// <summary>
        /// 分批次批量读取事件的时候每次读取的数据量
        /// </summary>
        protected virtual int NumberOfEventsPerRead => BaseOptions.NumberOfEventsPerRead;
        /// <summary>
        /// 快照的事件版本号
        /// </summary>
        protected long SnapshotEventVersion { get; private set; }
        /// <summary>
        /// 失活的时候保存快照的最小事件Version间隔
        /// </summary>
        protected virtual int MinSnapshotIntervaVersionl => BaseOptions.MinSnapshotIntervalVersion;
        /// <summary>
        /// 是否支持异步follow，true代表事件会广播，false事件不会进行广播
        /// </summary>
        protected virtual bool SupportFollow => true;
        /// <summary>
        /// 当前Grain的真实Type
        /// </summary>
        protected Type GrainType { get; }
        /// <summary>
        /// 依赖注入统一方法
        /// </summary>
        protected async virtual ValueTask DependencyInjection()
        {
            BaseOptions = ServiceProvider.GetService<IOptions<BaseOptions>>().Value;
            OverOptions = ServiceProvider.GetService<IOptions<OverOptions>>().Value;
            StorageFactory = ServiceProvider.GetService<IStorageFactoryContainer>().CreateFactory(GrainType);
            ProducerContainer = ServiceProvider.GetService<IProducerContainer>();
            Serializer = ServiceProvider.GetService<ISerializer>();
            JsonSerializer = ServiceProvider.GetService<IJsonSerializer>();
            EventHandler = ServiceProvider.GetService<IEventHandler<K, E, S, B>>();
            FollowUnit = ServiceProvider.GetService<IFollowUnitContainer>().GetUnit<K>(GrainType);
            ArchiveOptions = ServiceProvider.GetService<IOptions<ArchiveOptions>>().Value;
            //创建事件存储器
            var archiveStorageTask = StorageFactory.CreateArchiveStorage<K, S, B>(this, GrainId);
            if (!archiveStorageTask.IsCompleted)
                await archiveStorageTask;
            ArchiveStorage = archiveStorageTask.Result;
            //创建事件存储器
            var eventStorageTask = StorageFactory.CreateEventStorage<K, E>(this, GrainId);
            if (!eventStorageTask.IsCompleted)
                await eventStorageTask;
            EventStorage = eventStorageTask.Result;
            //创建状态存储器
            var stateStorageTask = StorageFactory.CreateSnapshotStorage<K, S, B>(this, GrainId);
            if (!stateStorageTask.IsCompleted)
                await stateStorageTask;
            SnapshotStorage = stateStorageTask.Result;
            //创建事件发布器
            var producerTask = ProducerContainer.GetProducer(this);
            if (!producerTask.IsCompleted)
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
            if (!dITask.IsCompleted)
                await dITask;
            try
            {
                if (ArchiveOptions.On)
                {
                    //加载归档信息
                    BriefArchiveList = await ArchiveStorage.GetBriefList(State.Base.StateId);
                    LastArchive = BriefArchiveList.LastOrDefault();
                    if (LastArchive != default && !IsCompletedArchive(LastArchive) && !LastArchive.EventIsCleared)
                    {
                        await ArchiveStorage.Delete(LastArchive.Id, State.Base.StateId);
                        BriefArchiveList.Remove(LastArchive);
                        NewArchive = LastArchive;
                        LastArchive = BriefArchiveList.LastOrDefault();
                    }
                }
                //修复状态
                await RecoveryState();

                if (ArchiveOptions.On)
                {
                    if (NewArchive != default && NewArchive.EndVersion < State.Base.Version)
                    {
                        //归档恢复
                        while (true)
                        {
                            var eventList = await EventStorage.GetList(GrainId, NewArchive.EndTimestamp, NewArchive.EndVersion, NewArchive.EndVersion + NumberOfEventsPerRead);
                            foreach (var @event in eventList)
                            {
                                var task = EventArchive(@event);
                                if (!task.IsCompleted)
                                    await task;
                            }
                            if (NewArchive.EndVersion == State.Base.Version) break;
                        };
                    }
                }
                var onActivatedTask = OnBaseActivated();
                if (!onActivatedTask.IsCompleted)
                    await onActivatedTask;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainActivateId, "Grain activation completed with id = {0}", GrainId.ToString());
            }
            catch (Exception ex)
            {
                Logger.LogCritical(LogEventIds.GrainActivateId, ex, "Grain activation failed with Id = {0}", GrainId.ToString());
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
        private bool IsCompletedArchive(ArchiveBrief briefArchive)
        {
            var intervalMilliseconds = briefArchive.EndTimestamp - briefArchive.StartTimestamp;
            var intervalVersiion = briefArchive.EndVersion - briefArchive.StartVersion;
            return (intervalMilliseconds > ArchiveOptions.IntervalMilliseconds &&
                intervalVersiion > ArchiveOptions.IntervalVersion) ||
                intervalMilliseconds > ArchiveOptions.MaxIntervalMilliSeconds ||
                intervalVersiion > ArchiveOptions.MaxIntervalVersion;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnBaseActivated() => Consts.ValueTaskDone;
        protected virtual async Task RecoveryState()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainStateRecoveryId, "The state of id = {0} begin to recover", GrainType.FullName, GrainId.ToString());
            try
            {
                var readSnapshotTask = ReadSnapshotAsync();
                if (!readSnapshotTask.IsCompleted)
                    await readSnapshotTask;
                while (!State.Base.IsLatest)
                {
                    var eventList = await EventStorage.GetList(GrainId, State.Base.Version, State.Base.LatestMinEventTimestamp, State.Base.Version + NumberOfEventsPerRead);
                    foreach (var @event in eventList)
                    {
                        State.IncrementDoingVersion(GrainType);//标记将要处理的Version
                        EventApply(State, @event);
                        State.UpdateVersion(@event, GrainType);//更新处理完成的Version
                    }
                    if (eventList.Count < NumberOfEventsPerRead) break;
                };
                State.Base.IsLatest = false;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainStateRecoveryId, "The state of id = {0} recovery has been completed ,state version = {1}", GrainId.ToString(), State.Base.Version);
            }
            catch (Exception ex)
            {
                Logger.LogCritical(LogEventIds.GrainStateRecoveryId, ex, "The state of id = {0} recovery has failed ,state version = {1}", GrainId.ToString(), State.Base.Version);
                throw;
            }
        }
        public override async Task OnDeactivateAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainDeactivateId, "Grain start deactivation with id = {0}", GrainId.ToString());
            var needSaveSnap = State.Base.Version - SnapshotEventVersion >= MinSnapshotIntervaVersionl;
            try
            {
                if (needSaveSnap)
                {
                    var saveTask = SaveSnapshotAsync(true, true);
                    if (!saveTask.IsCompleted)
                        await saveTask;
                    var onDeactivatedTask = OnDeactivated();
                    if (!onDeactivatedTask.IsCompleted)
                        await onDeactivatedTask;
                }
                if (ArchiveOptions.On)
                {
                    if (NewArchive.EndVersion - NewArchive.StartVersion >= ArchiveOptions.MinIntervalVersion)
                    {
                        var archiveTask = Archive(true, true);
                        if (!archiveTask.IsCompleted)
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
        /// <summary>
        ///  true:当前状态无快照,false:当前状态已经存在快照
        /// </summary>
        protected bool NoSnapshot { get; private set; }
        protected virtual async Task ReadSnapshotAsync()
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start read snapshot  with Id = {0} ,state version = {1}", GrainId.ToString(), State.Base.Version);
            try
            {
                //从快照中恢复状态
                State = await SnapshotStorage.Get(GrainId);
                if (State == default)
                {
                    //从归档中恢复状态
                    if (ArchiveOptions.On)
                    {
                        State = await ArchiveStorage.GetState(LastArchive.Id, GrainId);
                    }
                    NoSnapshot = true;
                    if (State == default)
                    {
                        //新建状态
                        var createTask = CreateState();
                        if (!createTask.IsCompleted)
                            await createTask;
                    }
                }
                SnapshotEventVersion = State.Base.Version;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace(LogEventIds.GrainSnapshot, "The snapshot of id = {0} read completed, state version = {1}", GrainId.ToString(), State.Base.Version);
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
            if (State.Base.Version != State.Base.DoingVersion)
                throw new StateInsecurityException(State.Base.StateId.ToString(), GrainType, State.Base.DoingVersion, State.Base.Version);
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start save snapshot  with Id = {0} ,state version = {1},save type = {2}", GrainId.ToString(), State.Base.Version, SnapshotProcessor.ToString());
            if (SnapshotProcessor == SnapshotProcessor.Master)
            {
                //如果版本号差超过设置则更新快照
                if (force || (State.Base.Version - SnapshotEventVersion >= SnapshotIntervalVersion))
                {
                    var oldLatestMinEventTimestamp = State.Base.LatestMinEventTimestamp;
                    try
                    {
                        var onSaveSnapshotTask = OnStartSaveSnapshot();
                        if (!onSaveSnapshotTask.IsCompleted)
                            await onSaveSnapshotTask;
                        State.Base.LatestMinEventTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        State.Base.IsLatest = isLatest;
                        if (NoSnapshot)
                        {
                            await SnapshotStorage.Insert(State);
                            SnapshotEventVersion = State.Base.Version;
                            NoSnapshot = false;
                        }
                        else
                        {
                            await SnapshotStorage.Update(State);
                            SnapshotEventVersion = State.Base.Version;
                        }
                        if (Logger.IsEnabled(LogLevel.Trace))
                            Logger.LogTrace(LogEventIds.GrainSnapshot, "The snapshot of id={0} save completed ,state version = {1}", GrainId.ToString(), State.Base.Version);
                    }
                    catch (Exception ex)
                    {
                        State.Base.LatestMinEventTimestamp = oldLatestMinEventTimestamp;
                        if (Logger.IsEnabled(LogLevel.Critical))
                            Logger.LogCritical(LogEventIds.GrainSnapshot, ex, "The snapshot of id= {0} save failed", GrainId.ToString());
                        throw;
                    }
                }
            }
        }
        protected async Task Over()
        {
            if (State.Base.IsOver)
                throw new StateIsOverException(State.Base.StateId.ToString(), GrainType);
            var versions = await Task.WhenAll(FollowUnit.GetAllVersionsFunc().Select(func => func(State.Base.StateId)));
            if (versions.Any(v => v < State.Base.Version))
            {
                throw new FollowNotCompletedException(GrainType.FullName, State.Base.StateId.ToString());
            }
            if (State.Base.Version != State.Base.DoingVersion)
                throw new StateInsecurityException(State.Base.StateId.ToString(), GrainType, State.Base.DoingVersion, State.Base.Version);
            State.Base.IsOver = true;
            State.Base.IsLatest = true;
            if (SnapshotEventVersion != State.Base.Version)
            {
                var saveTask = SaveSnapshotAsync(true, true);
                if (!saveTask.IsCompleted)
                    await saveTask;
            }
            else
            {
                await SnapshotStorage.Over(State.Base.StateId);
            }
            if (OverOptions.ClearEvent)
            {
                await EventStorage.Delete(State.Base.StateId, State.Base.Version);
            }
        }
        protected virtual ValueTask OnStartSaveSnapshot() => Consts.ValueTaskDone;
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask CreateState()
        {
            State = new S
            {
                Base = new B
                {
                    StateId = GrainId
                }
            };
            return Consts.ValueTaskDone;
        }
        /// <summary>
        /// 删除状态
        /// </summary>
        /// <returns></returns>
        protected async ValueTask DeleteState()
        {
            if (State.Base.IsOver)
                throw new StateIsOverException(State.Base.StateId.ToString(), GrainType);
            if (SnapshotEventVersion > 0)
            {
                await SnapshotStorage.Delete(GrainId);
                SnapshotEventVersion = 0;
            }
        }
        /// <summary>
        /// 事件存储器
        /// </summary>
        protected IEventStorage<K, E> EventStorage { get; private set; }
        /// <summary>
        /// 状态存储器
        /// </summary>
        protected ISnapshotStorage<K, S, B> SnapshotStorage { get; private set; }
        /// <summary>
        /// 事件发布器
        /// </summary>
        protected IProducer EventBusProducer { get; private set; }
        protected virtual async Task<bool> RaiseEvent(IEvent<K, E> @event, EventUID uniqueId = null)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainSnapshot, "Start raise event, grain Id ={0} and state version = {1},event type = {2} ,event ={3},uniqueueId= {4}", GrainId.ToString(), State.Base.Version, @event.GetType().FullName, JsonSerializer.Serialize(@event), uniqueId);
            if (State.Base.IsOver)
                throw new StateIsOverException(State.Base.StateId.ToString(), GrainType);
            try
            {
                @event.Base.StateId = GrainId;
                @event.Base.Version = State.Base.Version + 1;
                if (uniqueId == default) uniqueId = EventUID.Empty;
                if (string.IsNullOrEmpty(uniqueId.UID))
                    @event.Base.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                else
                    @event.Base.Timestamp = uniqueId.Timestamp;
                var startTask = OnRaiseStart(@event);
                if (!startTask.IsCompleted)
                    await startTask;
                State.IncrementDoingVersion(GrainType);//标记将要处理的Version
                using (var ms = new PooledMemoryStream())
                {
                    Serializer.Serialize(ms, @event);
                    var bytes = ms.ToArray();
                    if (await EventStorage.Append(@event, bytes, uniqueId.UID))
                    {
                        if (SupportFollow)
                        {
                            var data = new W
                            {
                                TypeName = @event.GetType().FullName,
                                Bytes = bytes
                            };
                            ms.Position = 0;
                            ms.SetLength(0);
                            Serializer.Serialize(ms, data);
                            //消息写入消息队列，以提供异步服务
                            EventApply(State, @event);
                            try
                            {
                                var publishTask = EventBusProducer.Publish(ms.ToArray(), GrainId.ToString());
                                if (!publishTask.IsCompleted)
                                    await publishTask;
                            }
                            catch (Exception ex)
                            {
                                if (Logger.IsEnabled(LogLevel.Error))
                                    Logger.LogError(LogEventIds.GrainRaiseEvent, ex, "EventBus error,state  Id ={0}, version ={1}", GrainId.ToString(), State.Base.Version);
                            }
                        }
                        else
                        {
                            EventApply(State, @event);
                        }
                        State.UpdateVersion(@event, GrainType);//更新处理完成的Version
                        var saveSnapshotTask = SaveSnapshotAsync();
                        if (!saveSnapshotTask.IsCompleted)
                            await saveSnapshotTask;
                        var task = OnRaiseSuccessed(@event, bytes);
                        if (!task.IsCompleted)
                            await task;
                        if (Logger.IsEnabled(LogLevel.Trace))
                            Logger.LogTrace(LogEventIds.GrainRaiseEvent, "Raise event successfully, grain Id= {0} and state version = {1}}", GrainId.ToString(), State.Base.Version);
                        return true;
                    }
                    else
                    {
                        if (Logger.IsEnabled(LogLevel.Information))
                            Logger.LogInformation(LogEventIds.GrainRaiseEvent, "Raise event failure because of idempotency limitation, grain Id = {0},state version = {1},event type = {2} with version = {3}", GrainId.ToString(), State.Base.Version, @event.GetType().FullName, @event.Base.Version);
                        var task = OnRaiseFailed(@event);
                        if (!task.IsCompleted)
                            await task;
                        State.DecrementDoingVersion();//还原doing Version
                    }
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.GrainRaiseEvent, ex, "Raise event produces errors, state Id = {0}, version ={1},event type = {2},event = {3}", GrainId.ToString(), State.Base.Version, @event.GetType().FullName, JsonSerializer.Serialize(@event));
                await RecoveryState();//还原状态
                throw;
            }
            return false;
        }

        protected virtual async ValueTask OnRaiseStart(IEvent<K, E> @event)
        {
            if (@event.Base.Timestamp < State.Base.LatestMinEventTimestamp)
            {
                await SnapshotStorage.UpdateLatestMinEventTimestamp(State.Base.StateId, @event.Base.Timestamp);
                State.Base.LatestMinEventTimestamp = @event.Base.Timestamp;
            }
            if (ArchiveOptions.EventClearOptions.On && @event.Base.Timestamp < LastArchive.EndTimestamp)
            {
                foreach (var archive in BriefArchiveList.OrderByDescending(v => v.Index).ToList())
                {
                    if (@event.Base.Timestamp < archive.EndTimestamp)
                    {
                        if (archive.EventIsCleared)
                            throw new EventIsClearedException(@event.GetType().FullName, JsonSerializer.Serialize(@event), archive.Index);
                        await ArchiveStorage.Delete(archive.Id, State.Base.StateId);
                        if (NewArchive != default)
                            NewArchive = CombineArchiveInfo(archive, NewArchive);
                        else
                            NewArchive = archive;
                        BriefArchiveList.Remove(archive);
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }
        protected virtual ValueTask OnRaiseSuccessed(IEvent<K, E> @event, byte[] bytes)
        {
            if (ArchiveOptions.On)
            {
                return EventArchive(@event);
            }
            return Consts.ValueTaskDone;
        }
        protected virtual ValueTask OnRaiseFailed(IEvent<K, E> @event)
        {
            if (ArchiveOptions.On)
            {
                return Archive();
            }
            return Consts.ValueTaskDone;
        }
        protected virtual void EventApply(S state, IEvent<K, E> evt)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace(LogEventIds.GrainRaiseEvent, "Start apply event, grain Id= {0} and state version is {1}},event type = {2},event = {3}", GrainId.ToString(), State.Base.Version, evt.GetType().FullName, JsonSerializer.Serialize(evt));
            EventHandler.Apply(state, evt);
        }

        protected async ValueTask EventArchive(IEvent<K, E> @event)
        {
            if (NewArchive == default)
            {
                NewArchive = new ArchiveBrief
                {
                    Id = await GrainFactory.GetGrain<IUID>(GrainType.FullName).NewUtcID(),
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
                NewArchive.EndTimestamp = @event.Base.Timestamp;
                NewArchive.EndVersion = @event.Base.Version;
            }
            if (ArchiveOptions.On)
            {
                var archiveTask = Archive();
                if (!archiveTask.IsCompleted)
                    await archiveTask;
            }
        }
        public ArchiveBrief CombineArchiveInfo(ArchiveBrief one, ArchiveBrief two)
        {
            if (two.StartTimestamp < one.StartTimestamp)
                one.StartTimestamp = two.StartTimestamp;
            if (two.StartVersion < one.StartVersion)
                one.StartVersion = two.StartVersion;
            if (two.EndTimestamp > one.EndTimestamp)
                one.EndTimestamp = two.EndTimestamp;
            if (two.EndVersion > one.EndVersion)
                one.EndVersion = two.EndVersion;
            return one;
        }

        protected async ValueTask Archive(bool force = false, bool isLatest = false)
        {
            if (State.Base.Version != State.Base.DoingVersion)
                throw new StateInsecurityException(State.Base.StateId.ToString(), GrainType, State.Base.DoingVersion, State.Base.Version);
            var intervalMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - LastArchive.EndTimestamp;
            var intervalVersiion = State.Base.Version - LastArchive.EndVersion;
            if (force || ((intervalMilliseconds > ArchiveOptions.IntervalMilliseconds &&
                intervalVersiion > ArchiveOptions.IntervalVersion) ||
                intervalMilliseconds > ArchiveOptions.MaxIntervalMilliSeconds ||
                intervalVersiion > ArchiveOptions.MaxIntervalVersion
                ))
            {
                var task = OnStartArchive();
                if (!task.IsCompleted)
                    await task;
                State.Base.IsLatest = isLatest;
                await ArchiveStorage.Insert(NewArchive, State);
                BriefArchiveList.Add(NewArchive);
                LastArchive = NewArchive;
                NewArchive = default;
                var onTask = OnArchiveCompleted();
                if (!onTask.IsCompleted)
                    await onTask;
            }
        }
        protected virtual async ValueTask OnArchiveCompleted()
        {
            //开始执行事件清理逻辑
            var noCleareds = BriefArchiveList.Where(a => !a.EventIsCleared).ToList();
            if (noCleareds.Count >= ArchiveOptions.EventClearOptions.IntervalArchive)
            {
                var minArchive = noCleareds.FirstOrDefault();
                if (minArchive != default)
                {
                    //清理归档对应的事件
                    await ArchiveStorage.EventIsClear(minArchive.Id);
                    minArchive.EventIsCleared = true;
                    //如果快照的版本小于需要清理的最大事件版本号，则保存快照
                    if (SnapshotEventVersion < minArchive.EndVersion)
                    {
                        var saveTask = SaveSnapshotAsync(true);
                        if (!saveTask.IsCompleted)
                            await saveTask;
                    }
                    await EventStorage.Delete(State.Base.StateId, minArchive.EndVersion);
                }
            }
            //只保留一个清理过事件的快照，其它的删除掉
            var cleareds = BriefArchiveList.Where(a => a.EventIsCleared).OrderBy(a => a.Index).ToArray();
            if (cleareds.Length > 1)
            {
                for (int i = 0; i < cleareds.Length - 1; i++)
                {
                    var task = ArchiveStorage.Delete(cleareds[i].Id, State.Base.StateId);
                    if (!task.IsCompleted)
                        await task;
                    BriefArchiveList.Remove(cleareds[i]);
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
                    var data = new W
                    {
                        TypeName = msg.GetType().FullName,
                        Bytes = ms.ToArray()
                    };
                    ms.Position = 0;
                    ms.SetLength(0);
                    Serializer.Serialize(ms, data);
                    var pubLishTask = EventBusProducer.Publish(ms.ToArray(), hashKey);
                    if (!pubLishTask.IsCompleted)
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
