using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Message;
using Ray.Core.MQ;
using Ray.Core.Utils;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public abstract class ESGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : IMessageWrapper, new()
    {
        protected S State { get; set; }
        protected abstract K GrainId { get; }
        protected virtual SnapshotType SnapshotType => SnapshotType.Master;
        protected virtual int SnapshotFrequency => 500;
        protected virtual int EventNumberPerRead => 2000;
        protected Int64 StateStorageVersion { get; set; }
        protected virtual int SnapshotMinFrequency => 20;
        protected virtual bool SupportAsync => true;
        protected Channel<EventTransactionWrap<K>> InputFlowChannel { get; set; }
        /// <summary>
        /// 是否开启输入批量处理
        /// </summary>
        protected virtual bool OpenInputBuffer => false;
        protected ILogger<ESGrain<K, S, W>> Logger { get; set; }
        #region LifeTime
        public override async Task OnActivateAsync()
        {
            Logger = ServiceProvider.GetService<ILogger<ESGrain<K, S, W>>>();
            if (OpenInputBuffer)
                InputFlowChannel = Channel.CreateUnbounded<EventTransactionWrap<K>>();
            await ReadSnapshotAsync();
            while (true)
            {
                var eventList = await (await GetEventStorage()).GetListAsync(GrainId, State.Version, State.Version + EventNumberPerRead, State.VersionTime);
                foreach (var @event in eventList)
                {
                    State.IncrementDoingVersion();//标记将要处理的Version
                    EventHandle.Apply(State, @event);
                    State.UpdateVersion(@event);//更新处理完成的Version
                }
                if (eventList.Count < EventNumberPerRead) break;
            };
        }
        public override async Task OnDeactivateAsync()
        {
            if (State.Version - StateStorageVersion >= SnapshotMinFrequency)
                await SaveSnapshotAsync(true);
        }
        #endregion
        #region State storage
        protected bool IsNew { get; set; } = false;
        protected virtual async ValueTask ReadSnapshotAsync()
        {
            State = await (await GetStateStorage()).GetByIdAsync(GrainId);
            if (State == null)
            {
                IsNew = true;
                await CreateState();
            }
            StateStorageVersion = State.Version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual async ValueTask SaveSnapshotAsync(bool force = false)
        {
            if (SnapshotType == SnapshotType.Master)
            {
                //如果版本号差超过设置则更新快照
                if (force || (State.Version - StateStorageVersion >= SnapshotFrequency))
                {
                    await OnSaveSnapshot();
                    if (IsNew)
                    {
                        await (await GetStateStorage()).InsertAsync(State);

                        StateStorageVersion = State.Version;
                        IsNew = false;
                    }
                    else
                    {
                        await (await GetStateStorage()).UpdateAsync(State);
                        StateStorageVersion = State.Version;
                    }
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task OnSaveSnapshot() => Task.CompletedTask;
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
        protected async ValueTask ClearStateAsync()
        {
            await (await GetStateStorage()).DeleteAsync(GrainId);
        }
        IStateStorage<S, K> _stateStorage;

        protected virtual async ValueTask<IStateStorage<S, K>> GetStateStorage()
        {
            if (_stateStorage == null)
            {
                _stateStorage = await ServiceProvider.GetService<IStorageContainer>().GetStateStorage<K, S>(GetType(), this);
            }
            return _stateStorage;
        }
        #endregion

        #region Event
        protected IEventStorage<K> _eventStorage;

        protected virtual async ValueTask<IEventStorage<K>> GetEventStorage()
        {
            if (_eventStorage == null)
            {
                _eventStorage = await ServiceProvider.GetService<IStorageContainer>().GetEventStorage<K, S>(GetType(), this);
            }
            return _eventStorage;
        }
        protected IMQService _mqService;

        protected virtual IMQService GetMQService()
        {
            if (_mqService == null)
            {
                _mqService = ServiceProvider.GetService<IMQServiceContainer>().GetService(GetType(), this);
            }
            return _mqService;
        }
        ISerializer _serializer;

        protected ISerializer GetSerializer()
        {
            if (_serializer == null)
            {
                _serializer = ServiceProvider.GetService<ISerializer>();
            }
            return _serializer;
        }
        protected abstract IEventHandle EventHandle { get; }
        #region Transaction
        protected bool beginTransaction = false;
        private DateTime beginTransactionTime;
        private List<EventSaveWrap<K>> transactionEventList;
        protected async ValueTask BeginTransaction()
        {
            if (beginTransaction)
            {
                if ((DateTime.UtcNow - beginTransactionTime).TotalMinutes > 1)
                {
                    await RollbackTransaction();//事务阻赛超过一分钟自动回滚
                }
                throw new Exception("The transaction already exists");
            }
            if (transactionEventList == null)
                transactionEventList = new List<EventSaveWrap<K>>();
            beginTransaction = true;
            beginTransactionTime = DateTime.UtcNow;
        }
        protected async ValueTask CommitTransaction()
        {
            if (transactionEventList.Count == 0) return;
            var serializer = GetSerializer();
            using (var ms = new PooledMemoryStream())
            {
                foreach (var @event in transactionEventList)
                {
                    serializer.Serialize(ms, @event.Evt);
                    @event.Bytes = ms.ToArray();
                    ms.Position = 0;
                    ms.SetLength(0);
                }
            }
            var saved = await (await GetEventStorage()).BatchSaveAsync(transactionEventList);
            if (saved)
            {
                if (SupportAsync)
                {
                    var mqService = GetMQService();
                    using (var ms = new PooledMemoryStream())
                    {
                        foreach (var @event in transactionEventList)
                        {
                            var data = new W
                            {
                                TypeCode = @event.Evt.TypeCode,
                                BinaryBytes = @event.Bytes
                            };
                            serializer.Serialize(ms, data);
                            //消息写入消息队列，以提供异步服务
                            await mqService.Publish(ms.ToArray(), @event.HashKey);
                            ms.Position = 0;
                            ms.SetLength(0);
                        }
                    }
                }
                await SaveSnapshotAsync();
            }
            transactionEventList.Clear();
            beginTransaction = false;
        }
        protected async ValueTask RollbackTransaction()
        {
            if (beginTransaction)
            {
                await OnActivateAsync();
                transactionEventList.Clear();
                beginTransaction = false;
            }
        }
        #endregion
        protected async ValueTask<bool> RaiseEvent(IEventBase<K> @event, string uniqueId = null, string hashKey = null, bool isTransaction = false)
        {
            try
            {
                State.IncrementDoingVersion();//标记将要处理的Version
                @event.StateId = GrainId;
                @event.Version = State.Version + 1;
                @event.Timestamp = DateTime.UtcNow;
                if (beginTransaction)
                {
                    if (!isTransaction)
                        throw new Exception("The transaction is in progress!");
                    transactionEventList.Add(new EventSaveWrap<K>(@event, null, string.IsNullOrEmpty(uniqueId) ? @event.GetUniqueId() : uniqueId, string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey));
                    EventHandle.Apply(State, @event);
                    State.UpdateVersion(@event);//更新处理完成的Version
                    return true;
                }
                else
                {
                    var serializer = GetSerializer();
                    using (var ms = new PooledMemoryStream())
                    {
                        serializer.Serialize(ms, @event);
                        var bytes = ms.ToArray();
                        var saved = await (await GetEventStorage()).SaveAsync(@event, bytes, string.IsNullOrEmpty(uniqueId) ? @event.GetUniqueId() : uniqueId);
                        if (saved)
                        {
                            if (SupportAsync)
                            {
                                var data = new W
                                {
                                    TypeCode = @event.TypeCode,
                                    BinaryBytes = bytes
                                };
                                ms.Position = 0;
                                ms.SetLength(0);
                                serializer.Serialize(ms, data);
                                //消息写入消息队列，以提供异步服务
                                await Task.WhenAll(Task.Factory.StartNew(() =>
                                {
                                    EventHandle.Apply(State, @event);
                                }), GetMQService().Publish(ms.ToArray(), string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey));
                            }
                            else
                            {
                                EventHandle.Apply(State, @event);
                            }
                            State.UpdateVersion(@event);//更新处理完成的Version
                            await SaveSnapshotAsync();
                            return true;
                        }
                        else
                            State.DecrementDoingVersion();//还原doing Version
                    }
                }
            }
            catch (Exception ex)
            {
                State.DecrementDoingVersion();//还原doing Version
                Logger.LogError(LogEventIds.EventRaiseError, ex, "Apply event {0} error, EventId={1}", @event.TypeCode, @event.Version);
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
            return false;
        }
        protected async ValueTask<bool> EnterBuffer(IEventBase<K> evt, string uniqueId = null)
        {
            var task = EventTransactionWrap<K>.Create(evt, uniqueId);
            await InputFlowChannel.Writer.WriteAsync(task);
            if (flowProcess == 0)
                TriggerInputFlow().GetAwaiter();
            return await task.TaskSource.Task;
        }
        int flowProcess = 0;
        protected async ValueTask TriggerInputFlow()
        {
            if (Interlocked.CompareExchange(ref flowProcess, 1, 0) == 0)
            {
                while (true)
                {
                    var (needTrigger, hasInput) = await FlowTriggerWaiting();
                    if (needTrigger)
                    {
                        while (await InputFlowBatchRaise())
                        {
                            await OnFlowNext(hasInput);
                        }
                    }
                    else
                    {
                        break;
                    }
                }
                Interlocked.Exchange(ref flowProcess, 0);
            }
        }
        protected virtual async ValueTask<(bool needTrigger, bool hasInput)> FlowTriggerWaiting()
        {
            return (await InputFlowChannel.Reader.WaitToReadAsync(), true);
        }
        protected virtual Task OnFlowNext(bool hasInput)
        {
            return Task.CompletedTask;
        }
        public async ValueTask<bool> InputFlowBatchRaise()
        {
            if (InputFlowChannel.Reader.TryRead(out var first))
            {
                var start = DateTime.UtcNow;
                var events = new List<EventTransactionWrap<K>>
                {
                    first
                };
                await BeginTransaction();
                try
                {
                    await RaiseEvent(first.Value, first.UniqueId, isTransaction: true);
                    while (InputFlowChannel.Reader.TryRead(out var data))
                    {
                        events.Add(data);
                        await RaiseEvent(data.Value, data.UniqueId, isTransaction: true);
                        if ((DateTime.UtcNow - start).TotalMilliseconds > 50) break;//保证批量延时不超过50ms
                    }

                    await CommitTransaction();
                    foreach (var evt in events)
                    {
                        evt.TaskSource.SetResult(true);
                    }
                }
                catch
                {
                    await RollbackTransaction();
                    await BatchEventReTry(events);
                }
                return true;
            }
            return false;
        }
        private async ValueTask BatchEventReTry(IList<EventTransactionWrap<K>> events)
        {
            foreach (var evt in events)
            {
                try
                {
                    evt.TaskSource.TrySetResult(await RaiseEvent(evt.Value, evt.UniqueId));
                }
                catch (Exception e)
                {
                    evt.TaskSource.TrySetException(e);
                }
            }
        }
        /// <summary>
        /// 发送无状态更改的消息到消息队列
        /// </summary>
        /// <returns></returns>
        public async ValueTask Publish(IMessage msg, string hashKey = null)
        {
            if (string.IsNullOrEmpty(hashKey))
                hashKey = GrainId.ToString();
            var serializer = GetSerializer();
            using (var ms = new PooledMemoryStream())
            {
                serializer.Serialize(ms, msg);
                var data = new W
                {
                    TypeCode = msg.TypeCode,
                    BinaryBytes = ms.ToArray()
                };
                ms.Position = 0;
                ms.SetLength(0);
                serializer.Serialize(ms, data);
                await GetMQService().Publish(ms.ToArray(), hashKey);
            }
        }
        #endregion
    }
}
