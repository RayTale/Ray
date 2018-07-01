using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Message;
using Ray.Core.MQ;
using Ray.Core.Utils;
using System;
using System.Runtime.CompilerServices;
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
        protected ILogger<ESGrain<K, S, W>> Logger { get; set; }
        #region LifeTime
        public override async Task OnActivateAsync()
        {
            Logger = ServiceProvider.GetService<ILogger<ESGrain<K, S, W>>>();
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
        public override Task OnDeactivateAsync()
        {
            return State.Version - StateStorageVersion >= SnapshotMinFrequency ? SaveSnapshotAsync(true) : Task.CompletedTask;
        }
        #endregion
        #region State storage
        protected bool IsNew { get; set; } = false;
        protected virtual async Task ReadSnapshotAsync()
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
        protected virtual async Task SaveSnapshotAsync(bool force = false)
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
        protected async Task ClearStateAsync()
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
        protected async ValueTask<bool> RaiseEvent(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            try
            {
                State.IncrementDoingVersion();//标记将要处理的Version
                @event.StateId = GrainId;
                @event.Version = State.Version + 1;
                @event.Timestamp = DateTime.UtcNow;
                var serializer = GetSerializer();
                using (var ms = new PooledMemoryStream())
                {
                    serializer.Serialize(ms, @event);
                    var bytes = ms.ToArray();
                    var result = await (await GetEventStorage()).SaveAsync(@event, bytes, uniqueId);
                    if (result)
                    {
                        EventHandle.Apply(State, @event);
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
                            await GetMQService().Publish(ms.ToArray(), string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey);
                        }
                        State.UpdateVersion(@event);//更新处理完成的Version
                        await SaveSnapshotAsync();
                        return true;
                    }
                    else
                        State.DecrementDoingVersion();//还原doing Version
                }
            }
            catch (Exception ex)
            {
                State.DecrementDoingVersion();//还原doing Version
                Logger.LogError(LogEventIds.EventRaiseError, ex, "Apply event {0} error, EventId={1}", @event.TypeCode, @event.Version);
                throw ex;
            }
            return false;
        }
        /// <summary>
        /// 发送无状态更改的消息到消息队列
        /// </summary>
        /// <returns></returns>
        public async Task Publish(IMessage msg, string hashKey = null)
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
