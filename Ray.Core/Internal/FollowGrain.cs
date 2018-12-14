using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.Exceptions;
using Ray.Core.Messaging;
using Ray.Core.Utils;

namespace Ray.Core.Internal
{
    public abstract class FollowGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : IMessageWrapper
    {
        public FollowGrain(ILogger logger)
        {
            Logger = logger;
        }
        protected RayConfigOptions ConfigOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected IJsonSerializer JsonSerializer { get; private set; }
        protected ISerializer Serializer { get; private set; }
        protected IStorageContainer StorageContainer { get; private set; }
        /// <summary>
        /// Memory state, restored by snapshot + Event play or replay
        /// </summary>
        protected S State { get; set; }
        public abstract K GrainId { get; }
        /// <summary>
        /// 是否需要保存快照
        /// </summary>
        protected virtual bool SaveSnapshot => true;
        /// <summary>
        /// Grain保存快照的事件Version间隔
        /// </summary>
        protected virtual int SnapshotVersionInterval => ConfigOptions.FollowSnapshotVersionInterval;
        /// <summary>
        /// Grain失活的时候保存快照的最小事件Version间隔
        /// </summary>
        protected virtual int SnapshotMinVersionInterval => ConfigOptions.FollowSnapshotMinVersionInterval;
        /// <summary>
        /// 分批次批量读取事件的时候每次读取的数据量
        /// </summary>
        protected virtual int NumberOfEventsPerRead => ConfigOptions.NumberOfEventsPerRead;
        /// <summary>
        /// 并发处理消息时，收集消息等待处理的最大延时
        /// </summary>
        protected virtual int MaxDelayOfBatchMilliseconds => ConfigOptions.MaxDelayOfBatchMilliseconds;
        /// <summary>
        /// 事件处理的超时时间
        /// </summary>
        protected virtual int EventAsyncProcessTimeoutSeconds => ConfigOptions.EventAsyncProcessTimeoutSeconds;
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
        protected virtual bool Concurrent => false;
        protected Type GrainType { get; private set; }
        private MpscChannel<IEventBase<K>, bool> tellChannel;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask<IEventStorage<K>> GetEventStorage()
        {
            return StorageContainer.GetEventStorage<K, S>(this);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask<IStateStorage<S, K>> GetStateStorage()
        {
            return StorageContainer.GetStateStorage<K, S>(this);
        }
        #region 初始化数据
        public override async Task OnActivateAsync()
        {
            GrainType = GetType();
            ConfigOptions = ServiceProvider.GetService<IOptions<RayConfigOptions>>().Value;
            StorageContainer = ServiceProvider.GetService<IStorageContainer>();
            Serializer = ServiceProvider.GetService<ISerializer>();
            JsonSerializer = ServiceProvider.GetService<IJsonSerializer>();
            if (Concurrent)
            {
                tellChannel = new MpscChannel<IEventBase<K>, bool>(ConcurrentTellProcess);
            }
            await ReadSnapshotAsync();
            if (FullyActive)
            {
                var eventStorageTask = GetEventStorage();
                if (!eventStorageTask.IsCompleted)
                    await eventStorageTask;
                while (true)
                {
                    var eventList = await eventStorageTask.Result.GetListAsync(GrainId, State.Version, State.Version + NumberOfEventsPerRead, State.VersionTime);
                    if (Concurrent)
                    {
                        await Task.WhenAll(eventList.Select(@event =>
                        {
                            var task = OnEventDelivered(@event);
                            if (!task.IsCompleted)
                                return task.AsTask();
                            else
                                return Task.CompletedTask;
                        }));
                        var lastEvt = eventList.Last();
                        State.UnsafeUpdateVersion(lastEvt.Version, lastEvt.Timestamp);
                    }
                    else
                    {
                        foreach (var @event in eventList)
                        {
                            State.IncrementDoingVersion(GrainType);//标记将要处理的Version
                            var task = OnEventDelivered(@event);
                            if (!task.IsCompleted)
                                await task;
                            State.UpdateVersion(@event, GrainType);//更新处理完成的Version
                        }
                    }
                    var saveTask = SaveSnapshotAsync();
                    if (!saveTask.IsCompleted)
                        await saveTask;
                    if (eventList.Count < NumberOfEventsPerRead) break;
                };
            }
            if (Logger.IsEnabled(LogLevel.Information))
            {
                Logger.LogInformation("Activated of {0} for {1}:{2}", this.GetType().FullName, GrainId.ToString(), JsonSerializer.Serialize(State));
            }
        }
        public override Task OnDeactivateAsync()
        {
            if (Concurrent)
                tellChannel.Complete();
            if (State.Version - SnapshotEventVersion >= SnapshotMinVersionInterval)
                return SaveSnapshotAsync(true).AsTask();
            else
                return Task.CompletedTask;
        }
        protected bool IsNew { get; set; }
        protected virtual async Task ReadSnapshotAsync()
        {
            var stateStorageTask = GetStateStorage();
            if (!stateStorageTask.IsCompleted)
                await stateStorageTask;
            State = await stateStorageTask.Result.GetByIdAsync(GrainId);
            if (State == null)
            {
                IsNew = true;
                var createTask = CreateState();
                if (!createTask.IsCompleted)
                    await createTask;
            }
            SnapshotEventVersion = State.Version;
        }
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask CreateState()
        {
            State = new S
            {
                StateId = GrainId
            };
            return new ValueTask(Task.CompletedTask);
        }
        #endregion
        public Task ConcurrentTell(byte[] bytes)
        {
            using (var wms = new MemoryStream(bytes))
            {
                var message = Serializer.Deserialize<W>(wms);
                using (var ems = new MemoryStream(message.Bytes))
                {
                    if (Serializer.Deserialize(TypeContainer.GetType(message.TypeName), ems) is IEventBase<K> @event)
                    {
                        if (@event.Version > State.Version)
                        {
                            return tellChannel.WriteAsync(@event);
                        }
                    }
                }
            }
            return Task.CompletedTask;
        }

        readonly List<IEventBase<K>> UnprocessedList = new List<IEventBase<K>>();
        readonly TimeoutException timeoutException = new TimeoutException($"{nameof(OnEventDelivered)} with timeouts in {nameof(ConcurrentTellProcess)}");
        private async Task ConcurrentTellProcess(BufferBlock<DataTaskWrapper<IEventBase<K>, bool>> reader)
        {
            var start = DateTime.UtcNow;
            var evtList = new List<IEventBase<K>>();
            var startVersion = State.Version;
            var startTime = State.VersionTime;
            if (UnprocessedList.Count > 0)
            {
                var startEvt = UnprocessedList.Last();
                startVersion = startEvt.Version;
                startTime = startEvt.Timestamp;
            }
            var maxVersion = startVersion;
            TaskCompletionSource<bool> maxRequest = default;
            try
            {
                while (reader.TryReceiveAll(out var msgs))
                {
                    foreach (var wrap in msgs)
                    {
                        if (wrap.Value.Version <= startVersion)
                        {
                            wrap.TaskSource.TrySetResult(true);
                        }
                        else
                        {
                            evtList.Add(wrap.Value);
                            if (wrap.Value.Version > maxVersion)
                            {
                                maxRequest?.TrySetResult(true);
                                maxVersion = wrap.Value.Version;
                                maxRequest = wrap.TaskSource;
                            }
                            else
                            {
                                wrap.TaskSource.TrySetResult(true);
                            }
                        }
                    }
                    if ((DateTime.UtcNow - start).TotalMilliseconds > MaxDelayOfBatchMilliseconds) break;//保证批量延时不超过200ms
                }
                var orderList = evtList.OrderBy(w => w.Version).ToList();
                if (orderList.Count > 0)
                {
                    var inputLast = orderList.Last();
                    if (startVersion + orderList.Count != inputLast.Version)
                    {
                        var eventStorageTask = GetEventStorage();
                        if (!eventStorageTask.IsCompleted)
                            await eventStorageTask;
                        var loadList = await eventStorageTask.Result.GetListAsync(GrainId, startVersion, inputLast.Version, startTime);
                        UnprocessedList.AddRange(loadList);
                    }
                    else
                    {
                        UnprocessedList.AddRange(orderList.Select(w => w));
                    }
                }
                if (UnprocessedList.Count > 0)
                {
                    using (var tokenSource = new CancellationTokenSource())
                    {
                        var tasks = UnprocessedList.Select(@event =>
                        {
                            var task = OnEventDelivered(@event);
                            if (!task.IsCompleted)
                                return task.AsTask();
                            else
                                return Task.CompletedTask;
                        });
                        var taskOne = Task.WhenAll(tasks);
                        using (var taskTwo = Task.Delay(EventAsyncProcessTimeoutSeconds, tokenSource.Token))
                        {
                            await Task.WhenAny(taskOne, taskTwo);
                            if (taskOne.Status == TaskStatus.RanToCompletion)
                            {
                                tokenSource.Cancel();
                                var lastEvt = UnprocessedList.Last();
                                State.UnsafeUpdateVersion(lastEvt.Version, lastEvt.Timestamp);
                                var saveTask = SaveSnapshotAsync();
                                if (!saveTask.IsCompleted)
                                    await saveTask;
                                UnprocessedList.Clear();
                                maxRequest?.TrySetResult(true);
                            }
                            else
                            {
                                maxRequest?.TrySetException(timeoutException);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                maxRequest?.TrySetException(e);
            }
        }

        public Task Tell(byte[] bytes)
        {
            using (var wms = new MemoryStream(bytes))
            {
                var message = Serializer.Deserialize<W>(wms);
                var tellTask = Tell(message);
                if (!tellTask.IsCompleted)
                    return tellTask.AsTask();
                else
                    return Task.CompletedTask;
            }
        }
        public async ValueTask Tell(W message)
        {
            using (var ems = new MemoryStream(message.Bytes))
            {
                if (Serializer.Deserialize(TypeContainer.GetType(message.TypeName), ems) is IEventBase<K> @event)
                {
                    if (@event.Version == State.Version + 1)
                    {
                        var onEventDeliveredTask = OnEventDelivered(@event);
                        if (!onEventDeliveredTask.IsCompleted)
                            await onEventDeliveredTask;
                        State.FullUpdateVersion(@event, GrainType);//更新处理完成的Version
                    }
                    else if (@event.Version > State.Version)
                    {
                        var eventStorageTask = GetEventStorage();
                        if (!eventStorageTask.IsCompleted)
                            await eventStorageTask;
                        var eventList = await eventStorageTask.Result.GetListAsync(GrainId, State.Version, @event.Version, State.VersionTime);
                        foreach (var item in eventList)
                        {
                            var onEventDeliveredTask = OnEventDelivered(item);
                            if (!onEventDeliveredTask.IsCompleted)
                                await onEventDeliveredTask;
                            State.FullUpdateVersion(item, GrainType);//更新处理完成的Version
                        }
                    }
                    if (@event.Version == State.Version + 1)
                    {
                        var onEventDeliveredTask = OnEventDelivered(@event);
                        if (!onEventDeliveredTask.IsCompleted)
                            await onEventDeliveredTask;
                        State.FullUpdateVersion(@event, GrainType);//更新处理完成的Version
                    }
                    if (@event.Version > State.Version)
                    {
                        throw new EventVersionNotMatchStateException(GrainId.ToString(), GetType(), @event.Version, State.Version);
                    }
                    await SaveSnapshotAsync();
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnEventDelivered(IEventBase<K> @event) => new ValueTask(Task.CompletedTask);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSaveSnapshot() => new ValueTask(Task.CompletedTask);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSavedSnapshot() => new ValueTask(Task.CompletedTask);
        protected virtual async ValueTask SaveSnapshotAsync(bool force = false)
        {
            if (SaveSnapshot)
            {
                if (force || (State.Version - SnapshotEventVersion >= SnapshotVersionInterval))
                {
                    var onSaveSnapshotTask = OnSaveSnapshot();//自定义保存项
                    if (!onSaveSnapshotTask.IsCompleted)
                        await onSaveSnapshotTask;
                    var getStateStorageTask = GetStateStorage();
                    if (!getStateStorageTask.IsCompleted)
                        await getStateStorageTask;
                    if (IsNew)
                    {
                        await getStateStorageTask.Result.InsertAsync(State);
                        IsNew = false;
                    }
                    else
                    {
                        await getStateStorageTask.Result.UpdateAsync(State);
                    }
                    SnapshotEventVersion = State.Version;
                    var onSavedSnapshotTask = OnSavedSnapshot();
                    if (!onSavedSnapshotTask.IsCompleted)
                        await onSavedSnapshotTask;
                }
            }
        }
    }
}
