using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.Message;
using Ray.Core.MQ;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Utils;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Ray.Core.EventSourcing
{
    public abstract class ESGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : MessageWrapper
    {
        Int64 storageVersion;
        protected S State
        {
            get;
            set;
        }
        protected abstract K GrainId { get; }
        protected ILogger<ESGrain<K, S, W>> Logger { get; set; }
        #region LifeTime
        public override async Task OnActivateAsync()
        {
            Logger = ServiceProvider.GetService<ILogger<ESGrain<K, S, W>>>();
            await ReadSnapshotAsync();
            while (true)
            {
                var eventList = await GetEventStorage().GetListAsync(GrainId, State.Version, State.Version + 1000, State.VersionTime);
                foreach (var @event in eventList)
                {
                    State.IncrementDoingVersion();//标记将要处理的Version
                    EventHandle.Apply(State, @event);
                    State.UpdateVersion(@event);//更新处理完成的Version
                }
                if (eventList.Count < 1000) break;
            };
        }
        #endregion
        #region State storage
        protected bool IsNew { get; set; } = false;
        protected virtual async Task ReadSnapshotAsync()
        {
            if (SnapshotType != SnapshotType.NoSnapshot)
            {
                State = await GetStateStore().GetByIdAsync(GrainId);
            }
            if (State == null)
            {
                IsNew = true;
                await InitState();
            }
            storageVersion = State.Version;
        }
        protected virtual SnapshotType SnapshotType { get { return SnapshotType.Master; } }
        protected virtual int SnapshotFrequency => 200;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual async Task SaveSnapshotAsync()
        {
            if (SnapshotType == SnapshotType.Master)
            {
                if (IsNew)
                {
                    await GetStateStore().InsertAsync(State);
                    storageVersion = State.Version;
                    IsNew = false;
                }
                //如果版本号差超过设置则更新快照
                else if (State.Version - storageVersion >= SnapshotFrequency)
                {
                    await GetStateStore().UpdateAsync(State);
                    storageVersion = State.Version;
                }
            }
        }
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual Task InitState()
        {
            State = new S
            {
                StateId = GrainId
            };
            return Task.CompletedTask;
        }
        protected async Task ClearStateAsync()
        {
            await GetStateStore().DeleteAsync(GrainId);
        }
        IStateStorage<S, K> _StateStore;

        private IStateStorage<S, K> GetStateStore()
        {
            if (_StateStore == null)
            {
                _StateStore = ServiceProvider.GetService<IStorageContainer>().GetStateStorage<K, S>(GetType(), this);
            }
            return _StateStore;
        }
        #endregion

        #region Event
        protected IEventStorage<K> _eventStorage;

        protected virtual IEventStorage<K> GetEventStorage()
        {
            if (_eventStorage == null)
            {
                _eventStorage = ServiceProvider.GetService<IStorageContainer>().GetEventStorage<K, S>(GetType(), this);
            }
            return _eventStorage;
        }
        protected IMQService _mqService;

        protected virtual IMQService GetMQService()
        {
            if (_mqService == null)
            {
                _mqService = ServiceProvider.GetService<IMQServiceContainer>().GetService(GetType());
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
        protected virtual bool PublishToMQ { get; set; } = true;
        protected abstract IEventHandle EventHandle { get; }
        protected async ValueTask<bool> RaiseEvent(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            try
            {
                State.IncrementDoingVersion();//标记将要处理的Version
                @event.StateId = GrainId;
                @event.Version = State.Version + 1;
                @event.Timestamp = DateTime.UtcNow;
                using (var ms = new PooledMemoryStream())
                {
                    GetSerializer().Serialize(ms, @event);
                    var bytes = ms.ToArray();
                    var result = await GetEventStorage().SaveAsync(@event, bytes, uniqueId);
                    if (result)
                    {
                        EventHandle.Apply(State, @event);

                        if (PublishToMQ)
                        {
                            if (string.IsNullOrEmpty(hashKey)) hashKey = GrainId.ToString();
                            //消息写入消息队列         
                            await GetMQService().Publish(@event, bytes, hashKey);
                            State.UpdateVersion(@event);//更新处理完成的Version
                        }
                        await SaveSnapshotAsync();
                        return true;
                    }
                    else
                        State.DecrementDoingVersion();//还原doing Version
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(LogCodes.EventRaiseError, ex, "applay event {0} error, eventId={1}", @event.TypeCode, @event.Version);
                await OnActivateAsync();//重新激活Actor
                throw ex;
            }
            return false;
        }
        /// <summary>
        /// 发送无状态更改的消息到消息队列
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public async Task Publish(IActorOwnMessage<K> msg)
        {
            msg.StateId = GrainId;
            using (var ms = new PooledMemoryStream())
            {
                GetSerializer().Serialize(ms, msg);
                await GetMQService().Publish(msg, ms.ToArray(), GrainId.ToString());
            }
        }
        #endregion
    }
}
