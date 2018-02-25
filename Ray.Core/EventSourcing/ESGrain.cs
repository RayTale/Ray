using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.Message;
using Ray.Core.MQ;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Utils;
using System.Runtime.CompilerServices;

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

        #region LifeTime
        public override async Task OnActivateAsync()
        {
            await ReadSnapshotAsync();
            while (true)
            {
                var eventList = await EventStorage.GetListAsync(this.GrainId, this.State.Version, this.State.Version + 1000, this.State.VersionTime);
                foreach (var @event in eventList)
                {
                    this.State.IncrementDoingVersion();//标记将要处理的Version
                    EventHandle.Apply(this.State, @event.Event);
                    if (!@event.IsComplete)
                    {
                        using (var ms = new PooledMemoryStream())
                        {
                            Serializer.Serialize(ms, @event.Event);
                            await AfterEventSavedHandle(@event.Event, ms.ToArray());
                        }
                    }
                    this.State.UpdateVersion(@event.Event);//更新处理完成的Version
                }
                if (eventList.Count < 1000) break;
            };
        }
        Type thisType = null;
        private Type ThisType
        {
            get
            {
                if (thisType == null)
                {
                    thisType = this.GetType();
                }
                return thisType;
            }
        }
        #endregion
        #region State storage
        protected bool IsNew = false;
        protected virtual async Task ReadSnapshotAsync()
        {
            if (SnapshotType != SnapshotType.NoSnapshot)
            {
                this.State = await StateStore.GetByIdAsync(GrainId);
            }
            if (this.State == null)
            {
                IsNew = true;
                await InitState();
            }
            storageVersion = this.State.Version;
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
                    await StateStore.InsertAsync(this.State);
                    storageVersion = this.State.Version;
                    IsNew = false;
                }
                //如果版本号差超过设置则更新快照
                else if (this.State.Version - storageVersion >= SnapshotFrequency)
                {
                    await StateStore.UpdateAsync(this.State);
                    storageVersion = this.State.Version;
                }
            }
        }
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual Task InitState()
        {
            this.State = new S();
            this.State.StateId = GrainId;
            return Task.CompletedTask;
        }
        protected async Task ClearStateAsync()
        {
            await StateStore.DeleteAsync(GrainId);
        }
        IStateStorage<S, K> _StateStore;
        private IStateStorage<S, K> StateStore
        {
            get
            {
                if (_StateStore == null)
                {
                    _StateStore = ServiceProvider.GetService<IStorageContainer>().GetStateStorage<K, S>(ThisType, this);
                }
                return _StateStore;
            }
        }
        #endregion

        #region Event
        protected IEventStorage<K> _eventStorage;
        protected virtual IEventStorage<K> EventStorage
        {
            get
            {
                if (_eventStorage == null)
                {
                    _eventStorage = ServiceProvider.GetService<IStorageContainer>().GetEventStorage<K, S>(ThisType, this);
                }
                return _eventStorage;
            }
        }
        protected IMQService _mqService;
        protected virtual IMQService MQService
        {
            get
            {
                if (_mqService == null)
                {
                    _mqService = ServiceProvider.GetService<IMQServiceContainer>().GetService(ThisType);
                }
                return _mqService;
            }
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
        protected abstract IEventHandle EventHandle { get; }
        protected async ValueTask<bool> RaiseEvent(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            try
            {
                this.State.IncrementDoingVersion();//标记将要处理的Version
                @event.StateId = GrainId;
                @event.Version = this.State.Version + 1;
                @event.Timestamp = DateTime.UtcNow;
                using (var ms = new PooledMemoryStream())
                {
                    Serializer.Serialize(ms, @event);
                    var bytes = ms.ToArray();
                    var result = await EventStorage.SaveAsync(@event, bytes, uniqueId);
                    if (result)
                    {
                        EventHandle.Apply(this.State, @event);
                        await AfterEventSavedHandle(@event, bytes, hashKey: hashKey);

                        this.State.UpdateVersion(@event);//更新处理完成的Version

                        await SaveSnapshotAsync();
                        return true;
                    }
                    else
                        this.State.DecrementDoingVersion();//还原doing Version
                }
            }
            catch (Exception ex)
            {
                this.GetLogger("Event_Raise").Log(LogCodes.EventRaiseError, Orleans.Runtime.Severity.Error, $"applay event {@event.TypeCode} error, eventId={@event.Version}", null, ex);
                await OnActivateAsync();//重新激活Actor
                throw ex;
            }
            return false;
        }
        protected virtual bool PublishEventToMq { get { return true; } }
        protected virtual async Task AfterEventSavedHandle(IEventBase<K> @event, byte[] bytes, string hashKey = null)
        {
            try
            {
                if (string.IsNullOrEmpty(hashKey)) hashKey = GrainId.ToString();
                //消息写入消息队列         
                if (PublishEventToMq)
                    await MQService.Publish(@event, bytes, hashKey);
                //更改消息状态
                await EventStorage.CompleteAsync(@event);
            }
            catch (Exception e)
            {
                this.GetLogger("Event_Raise").Log(LogCodes.EventCompleteError, Orleans.Runtime.Severity.Error, "事件complate操作出现致命异常:" + string.Format("Grain类型={0},GrainId={1},StateId={2},Version={3},错误信息:{4}", ThisType.FullName, GrainId, @event.StateId, @event.Version, e.Message), null, e);
                throw e;
            }
        }
        /// <summary>
        /// 发送无状态更改的消息到消息队列
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public async Task Publish(IActorOwnMessage<K> msg)
        {
            msg.StateId = this.GrainId;
            using (var ms = new PooledMemoryStream())
            {
                Serializer.Serialize(ms, msg);
                await MQService.Publish(msg, ms.ToArray(), GrainId.ToString());
            }
        }
        #endregion
    }
}
