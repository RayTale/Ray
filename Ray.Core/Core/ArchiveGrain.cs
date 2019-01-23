using System;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
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

namespace Ray.Core.Core
{
    public abstract class ArchiveGrain<K, E, S, AS, B, W> : RayGrain<K, E, S, B, W>
        where E : IEventBase<K>
        where S : class, IState<K, B>, new()
        where AS : IStateArchive<K, S, B>, new()
        where B : IStateBase<K>, new()
        where W : IBytesWrapper, new()
    {
        /// <summary>
        /// 归档存储器
        /// </summary>
        protected IArchiveStorage<K, S, B> ArchiveStorage { get; private set; }
        protected ArchiveOptions ArchiveOptions { get; private set; }
        protected ArchiveEventClearOptions ArchiveEventClearOptions { get; private set; }
        protected List<BriefArchive> BriefArchiveList { get; private set; }
        protected BriefArchive FirstArchive { get; set; }
        protected BriefArchive LastArchive { get; private set; }
        protected BriefArchive NewArchive { get; private set; }
        public ArchiveGrain(ILogger logger) : base(logger)
        {
        }
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            //加载归档信息
            BriefArchiveList = await ArchiveStorage.GetBriefList(State.Base.StateId);
            FirstArchive = BriefArchiveList.FirstOrDefault();
            LastArchive = BriefArchiveList.LastOrDefault();
            if (!IsCompletedArchive(LastArchive))
            {
                await ArchiveStorage.Delete(LastArchive.Id, State.Base.StateId);
                BriefArchiveList.Remove(LastArchive);
                NewArchive = LastArchive;
                LastArchive = BriefArchiveList.LastOrDefault();
            }
            if (NewArchive != default && NewArchive.EndVersion < State.Base.Version)
            {
                //归档恢复
                while (true)
                {
                    var eventList = await EventStorage.GetList(GrainId, NewArchive.EndVersion, NewArchive.EndVersion + NumberOfEventsPerRead);
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
        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            if (ArchiveOptions.On)
            {
                if (NewArchive.EndVersion - NewArchive.StartVersion >= ArchiveOptions.MinIntervalVersion)
                {
                    var archiveTask = Archive();
                    if (!archiveTask.IsCompleted)
                        await archiveTask;
                }
            }
        }
        private bool IsCompletedArchive(BriefArchive briefArchive)
        {
            var intervalMilliseconds = briefArchive.EndTimestamp - briefArchive.StartTimestamp;
            var intervalVersiion = briefArchive.EndVersion - briefArchive.StartVersion;
            return (intervalMilliseconds > ArchiveOptions.IntervalMilliseconds &&
                intervalVersiion > ArchiveOptions.IntervalVersion) ||
                intervalMilliseconds > ArchiveOptions.MaxIntervalMilliSeconds ||
                intervalVersiion > ArchiveOptions.MaxIntervalVersion;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual async ValueTask OnArchiveCompleted()
        {
            //开始执行事件清理逻辑
            if (ArchiveEventClearOptions.On)
            {
                var noCleareds = BriefArchiveList.Where(a => !a.EventIsCleared).ToList();
                if (noCleareds.Count >= ArchiveEventClearOptions.IntervalArchive)
                {
                    var minArchive = noCleareds.FirstOrDefault();
                    if (minArchive != default)
                    {
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
            }
        }
        protected override async ValueTask OnRaiseStart(IEvent<K, E> @event)
        {
            foreach (var archive in BriefArchiveList.OrderByDescending(v => v.Index).ToList())
            {
                if (@event.Base.Timestamp < archive.EndTimestamp)
                {
                    if (archive.EventIsCleared)
                        throw new EventIsClearedException(@event.GetType().FullName, JsonSerializer.Serialize(@event), archive.Index);
                    await ArchiveStorage.Delete(LastArchive.Id, State.Base.StateId);
                    NewArchive = CombineArchiveInfo(archive, NewArchive);
                    await ArchiveStorage.Insert(NewArchive, State);
                    BriefArchiveList.Remove(archive);
                    BriefArchiveList.Add(NewArchive);
                    NewArchive = default;
                    LastArchive = BriefArchiveList.LastOrDefault();
                    FirstArchive = BriefArchiveList.FirstOrDefault();
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override ValueTask OnRaiseSuccess(IEvent<K, E> @event, byte[] bytes)
        {
            return EventArchive(@event);
        }
        protected async ValueTask EventArchive(IEvent<K, E> @event)
        {
            if (NewArchive == default)
            {
                NewArchive = new BriefArchive
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
        public BriefArchive CombineArchiveInfo(BriefArchive one, BriefArchive two)
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
        protected override async ValueTask DependencyInjection()
        {
            ArchiveOptions = ServiceProvider.GetService<IOptions<ArchiveOptions>>().Value;
            ArchiveEventClearOptions = ServiceProvider.GetService<IOptions<ArchiveEventClearOptions>>().Value;
            //创建事件存储器
            var archiveStorageTask = StorageFactory.CreateArchiveStorage<K, S, B>(this, GrainId);
            if (!archiveStorageTask.IsCompleted)
                await archiveStorageTask;
            ArchiveStorage = archiveStorageTask.Result;
            //父级依赖注入
            var baseTask = base.DependencyInjection();
            if (!baseTask.IsCompleted)
                await baseTask;
        }
        protected async ValueTask Archive()
        {
            if (State.Base.Version != State.Base.DoingVersion)
                throw new StateInsecurityException(State.Base.StateId.ToString(), GrainType, State.Base.DoingVersion, State.Base.Version);
            var intervalMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - LastArchive.EndTimestamp;
            var intervalVersiion = State.Base.Version - LastArchive.EndVersion;
            if ((intervalMilliseconds > ArchiveOptions.IntervalMilliseconds &&
                intervalVersiion > ArchiveOptions.IntervalVersion) ||
                intervalMilliseconds > ArchiveOptions.MaxIntervalMilliSeconds ||
                intervalVersiion > ArchiveOptions.MaxIntervalVersion
                )
            {
                await ArchiveStorage.Insert(NewArchive, State);
                if (FirstArchive == default)
                    FirstArchive = NewArchive;
                BriefArchiveList.Add(NewArchive);
                LastArchive = NewArchive;
                NewArchive = default;
                var onTask = OnArchiveCompleted();
                if (!onTask.IsCompleted)
                    await onTask;
            }
        }
    }
}
