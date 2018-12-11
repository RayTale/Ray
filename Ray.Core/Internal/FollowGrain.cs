using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Messaging;
using Ray.Core.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Ray.Core.Internal
{
    public abstract class FollowGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : IMessageWrapper
    {
        readonly ILogger logger;
        readonly IJsonSerializer jsonSerializer;
        protected ISerializer serializer;
        protected IStorageContainer storageContainer;
        public FollowGrain(
            ILogger logger,
            ISerializer serializer,
            IStorageContainer storageContainer,
            IJsonSerializer jsonSerializer
            )
        {
            this.logger = logger;
            this.serializer = serializer;
            this.storageContainer = storageContainer;
            this.jsonSerializer = jsonSerializer;
        }
        /// <summary>
        /// Memory state, restored by snapshot + Event play or replay
        /// </summary>
        protected S State { get; set; }
        public abstract K GrainId { get; }
        protected virtual bool SaveSnapshot => true;
        protected virtual int SnapshotFrequency => 20;
        protected virtual int EventNumberPerRead => 2000;
        /// <summary>
        /// 并发处理消息时，消息收集的最大延时
        /// </summary>
        protected virtual int ConcurrentMaxDelayMilliseconds => 200;
        /// <summary>
        /// 处理的超时时间
        /// </summary>
        protected virtual int ProcessTimeoutMilliseconds => 30 * 1000;
        protected virtual bool FullyActive => false;
        protected long StateStorageVersion { get; set; }
        protected virtual int SnapshotMinFrequency => 1;
        protected virtual bool Concurrent => false;
        private readonly DataBatchChannel<IEventBase<K>, bool> tellChannel;
        protected virtual ValueTask<IEventStorage<K>> GetEventStorage()
        {
            return storageContainer.GetEventStorage<K, S>(this);
        }

        protected virtual ValueTask<IStateStorage<S, K>> GetStateStorage()
        {
            return storageContainer.GetStateStorage<K, S>(this);
        }
        public FollowGrain()
        {
            if (Concurrent)
            {
                tellChannel = new DataBatchChannel<IEventBase<K>, bool>(ConcurrentTellProcess);
            }
        }
        #region 初始化数据
        public override async Task OnActivateAsync()
        {
            await ReadSnapshotAsync();
            if (FullyActive)
            {
                var eventStorageTask = GetEventStorage();
                if (!eventStorageTask.IsCompleted)
                    await eventStorageTask;
                while (true)
                {
                    var eventList = await eventStorageTask.Result.GetListAsync(GrainId, State.Version, State.Version + EventNumberPerRead, State.VersionTime);
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
                            State.IncrementDoingVersion();//标记将要处理的Version
                            var task = OnEventDelivered(@event);
                            if (!task.IsCompleted)
                                await task;
                            State.UpdateVersion(@event);//更新处理完成的Version
                        }
                    }
                    var saveTask = SaveSnapshotAsync();
                    if (!saveTask.IsCompleted)
                        await saveTask;
                    if (eventList.Count < EventNumberPerRead) break;
                };
            }
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace("Activated of {0} for {1}:{2}", this.GetType().FullName, GrainId.ToString(), jsonSerializer.Serialize(State));
            }
        }
        public override Task OnDeactivateAsync()
        {
            if (Concurrent)
                tellChannel.Complete();
            if (State.Version - StateStorageVersion >= SnapshotMinFrequency)
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
            StateStorageVersion = State.Version;
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
                var message = serializer.Deserialize<W>(wms);
                if (TypeContainer.TryGetValue(message.TypeName, out var type))
                {
                    using (var ems = new MemoryStream(message.Bytes))
                    {
                        if (serializer.Deserialize(type, ems) is IEventBase<K> @event)
                        {
                            if (@event.Version > State.Version)
                            {
                                return tellChannel.WriteAsync(@event);
                            }
                        }
                    }
                }
            }
            return Task.CompletedTask;
        }

        readonly List<IEventBase<K>> UnprocessedList = new List<IEventBase<K>>();
        readonly TimeoutException timeoutException = new TimeoutException(nameof(OnEventDelivered));
        private async Task ConcurrentTellProcess(BufferBlock<DataTaskWrap<IEventBase<K>, bool>> reader)
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
                    if ((DateTime.UtcNow - start).TotalMilliseconds > ConcurrentMaxDelayMilliseconds) break;//保证批量延时不超过200ms
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
                        using (var taskTwo = Task.Delay(ProcessTimeoutMilliseconds, tokenSource.Token))
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
                var message = serializer.Deserialize<W>(wms);
                var tellTask = Tell(message);
                if (!tellTask.IsCompleted)
                    return tellTask.AsTask();
                else
                    return Task.CompletedTask;
            }
        }
        public async ValueTask Tell(W message)
        {
            if (TypeContainer.TryGetValue(message.TypeName, out var type))
            {
                using (var ems = new MemoryStream(message.Bytes))
                {
                    if (serializer.Deserialize(type, ems) is IEventBase<K> @event)
                    {
                        if (@event.Version == State.Version + 1)
                        {
                            var onEventDeliveredTask = OnEventDelivered(@event);
                            if (!onEventDeliveredTask.IsCompleted)
                                await onEventDeliveredTask;
                            State.FullUpdateVersion(@event);//更新处理完成的Version
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
                                State.FullUpdateVersion(item);//更新处理完成的Version
                            }
                        }
                        if (@event.Version == State.Version + 1)
                        {
                            var onEventDeliveredTask = OnEventDelivered(@event);
                            if (!onEventDeliveredTask.IsCompleted)
                                await onEventDeliveredTask;
                            State.FullUpdateVersion(@event);//更新处理完成的Version
                        }
                        if (@event.Version > State.Version)
                        {
                            throw new Exception($"Event version of the error,Type={GetType().FullName},StateId={this.GrainId.ToString()},StateVersion={State.Version},EventVersion={@event.Version}");
                        }
                        await SaveSnapshotAsync();
                    }
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
                if (force || (State.Version - StateStorageVersion >= SnapshotFrequency))
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
                    StateStorageVersion = State.Version;
                    var onSavedSnapshotTask = OnSavedSnapshot();
                    if (!onSavedSnapshotTask.IsCompleted)
                        await onSavedSnapshotTask;
                }
            }
        }
    }
}
