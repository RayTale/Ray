using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Message;
using Ray.Core.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Ray.Core.EventSourcing
{
    public abstract class AsyncGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : IMessageWrapper
    {
        protected S State { get; set; }
        protected abstract K GrainId { get; }
        protected virtual bool SaveSnapshot => true;
        protected virtual int SnapshotFrequency => 20;
        protected virtual int EventNumberPerRead => 2000;
        protected virtual bool FullyActive => false;
        protected long StateStorageVersion { get; set; }
        protected virtual int SnapshotMinFrequency => 1;
        protected virtual bool Concurrent => false;
        #region concurrent variable
        private DataBatchChannel<IEventBase<K>, bool> tellChannel;
        IEventStorage<K> _eventStorage;
        #endregion
        protected async ValueTask<IEventStorage<K>> GetEventStorage()
        {
            if (_eventStorage == null)
            {
                _eventStorage = await ServiceProvider.GetService<IStorageContainer>().GetEventStorage<K, S>(GetType(), this);
            }
            return _eventStorage;
        }
        IStateStorage<S, K> _StateStore;
        private async ValueTask<IStateStorage<S, K>> GetStateStore()
        {
            if (_StateStore == null)
            {
                _StateStore = await ServiceProvider.GetService<IStorageContainer>().GetStateStorage<K, S>(GetType(), this);
            }
            return _StateStore;
        }
        ISerializer _serializer;
        protected ISerializer Serializer
        {
            get
            {
                if (_serializer == null)
                {
                    _serializer = ServiceProvider.GetService<ISerializer>();
                }
                return _serializer;
            }
        }
        public AsyncGrain()
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
                while (true)
                {
                    var eventList = await (await GetEventStorage()).GetListAsync(GrainId, State.Version, State.Version + EventNumberPerRead, State.VersionTime);
                    if (Concurrent)
                    {
                        await Task.WhenAll(eventList.Select(@event => OnEventDelivered(@event)));
                        var lastEvt = eventList.Last();
                        State.UnsafeUpdateVersion(lastEvt.Version, lastEvt.Timestamp);
                    }
                    else
                    {
                        foreach (var @event in eventList)
                        {
                            State.IncrementDoingVersion();//标记将要处理的Version
                            await OnEventDelivered(@event);
                            State.UpdateVersion(@event);//更新处理完成的Version
                        }
                    }
                    await SaveSnapshotAsync();
                    if (eventList.Count < EventNumberPerRead) break;
                };
            }
        }
        public override Task OnDeactivateAsync()
        {
            if (Concurrent)
                tellChannel.Complete();
            return State.Version - StateStorageVersion >= SnapshotMinFrequency ? SaveSnapshotAsync(true) : Task.CompletedTask;
        }
        protected bool IsNew { get; set; }
        protected virtual async Task ReadSnapshotAsync()
        {
            State = await (await GetStateStore()).GetByIdAsync(GrainId);
            if (State == null)
            {
                IsNew = true;
                await CreateState();
            }
            StateStorageVersion = State.Version;
        }
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual Task CreateState()
        {
            State = new S
            {
                StateId = GrainId
            };
            return Task.CompletedTask;
        }
        #endregion
        public async Task ConcurrentTell(byte[] bytes)
        {
            using (var wms = new MemoryStream(bytes))
            {
                var message = Serializer.Deserialize<W>(wms);
                if (MessageTypeMapper.EventTypeDict.TryGetValue(message.TypeCode, out var type))
                {
                    using (var ems = new MemoryStream(message.BinaryBytes))
                    {
                        if (Serializer.Deserialize(type, ems) is IEventBase<K> @event)
                        {
                            if (@event.Version > State.Version)
                            {
                                await tellChannel.WriteAsync(@event);
                            }
                        }
                    }
                }
            }
        }
        List<IEventBase<K>> UnprocessedList = new List<IEventBase<K>>();
        readonly Exception timeoutException = new Exception("Message processing timeout");
        private async ValueTask ConcurrentTellProcess(BufferBlock<DataTaskWrap<IEventBase<K>, bool>> reader)
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
                    if ((DateTime.UtcNow - start).TotalMilliseconds > 200) break;//保证批量延时不超过200ms
                }
                var orderList = evtList.OrderBy(w => w.Version).ToList();
                if (orderList.Count > 0)
                {
                    var inputLast = orderList.Last();
                    if (startVersion + orderList.Count != inputLast.Version)
                    {
                        var loadList = await (await GetEventStorage()).GetListAsync(GrainId, startVersion, inputLast.Version, startTime);
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
                        var tasks = UnprocessedList.Select(@event => OnEventDelivered(@event));
                        var taskOne = Task.WhenAll(tasks);
                        using (var taskTwo = Task.Delay(3 * 60 * 1000, tokenSource.Token))
                        {
                            await Task.WhenAny(taskOne, taskTwo);
                            if (taskOne.Status == TaskStatus.RanToCompletion)
                            {
                                tokenSource.Cancel();
                                var lastEvt = UnprocessedList.Last();
                                State.UnsafeUpdateVersion(lastEvt.Version, lastEvt.Timestamp);
                                await SaveSnapshotAsync();
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
                return Tell(message);
            }
        }
        public async Task Tell(W message)
        {
            if (MessageTypeMapper.EventTypeDict.TryGetValue(message.TypeCode, out var type))
            {
                using (var ems = new MemoryStream(message.BinaryBytes))
                {
                    if (Serializer.Deserialize(type, ems) is IEventBase<K> @event)
                    {
                        if (@event.Version == State.Version + 1)
                        {
                            await OnEventDelivered(@event);
                            State.FullUpdateVersion(@event);//更新处理完成的Version
                        }
                        else if (@event.Version > State.Version)
                        {
                            var eventList = await (await GetEventStorage()).GetListAsync(GrainId, State.Version, @event.Version, State.VersionTime);
                            foreach (var item in eventList)
                            {
                                await OnEventDelivered(item);
                                State.FullUpdateVersion(item);//更新处理完成的Version
                            }
                        }
                        if (@event.Version == State.Version + 1)
                        {
                            await OnEventDelivered(@event);
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
        protected virtual Task OnEventDelivered(IEventBase<K> @event) => Task.CompletedTask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task OnSaveSnapshot() => Task.CompletedTask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task OnSavedSnapshot() => Task.CompletedTask;
        protected virtual async Task SaveSnapshotAsync(bool force = false)
        {
            if (SaveSnapshot)
            {
                if (force || (State.Version - StateStorageVersion >= SnapshotFrequency))
                {
                    await OnSaveSnapshot();//自定义保存项
                    if (IsNew)
                    {
                        await (await GetStateStore()).InsertAsync(State);
                        IsNew = false;
                    }
                    else
                    {
                        await (await GetStateStore()).UpdateAsync(State);
                    }
                    StateStorageVersion = State.Version;
                    await OnSavedSnapshot();
                }
            }
        }
    }
}
