using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.Message;
using Ray.Core.MQ;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Lib;
using System.IO;
using System.Runtime.CompilerServices;

namespace Ray.Core.EventSourcing
{
    public abstract class ESGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : MessageWrapper
    {
        UInt32 storageVersion;
        protected S State
        {
            get;
            set;
        }
        protected abstract K GrainId { get; }
        protected virtual bool SendEventToMQ { get { return true; } }
        #region LifeTime
        public override async Task OnActivateAsync()
        {
            await ReadSnapshotAsync();
            while (true)
            {
                var eventList = await EventStorage.GetListAsync(this.GrainId, this.State.Version, this.State.Version + 1000, this.State.VersionTime);
                foreach (var @event in eventList)
                {
                    @event.Event.Apply(this.State);
                    if (!@event.IsComplete)
                    {
                        using (var ms = new MemoryStream())
                        {
                            Serializer.Serialize(ms, @event.Event);
                            await EventInsertAfterHandle(@event.Event, ms.ToArray());
                        }
                    }
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
        protected virtual int SnapshotFrequency { get { return 50; } }
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual void RaiseEventAfter(IEventBase<K> @event, byte[] bytes)
        {
        }
        protected async Task<bool> RaiseEvent(IEventBase<K> @event, string uniqueId = null, string mqHashKey = null)
        {
            try
            {
                @event.StateId = GrainId;
                @event.Version = this.State.Version + 1;
                @event.Timestamp = DateTime.Now;
                using (var ms = new MemoryStream())
                {
                    Serializer.Serialize(ms, @event);
                    var bytes = ms.ToArray();
                    var result = await EventStorage.InsertAsync(@event, bytes, uniqueId, SendEventToMQ);
                    if (result)
                    {
                        @event.Apply(this.State);
                        await EventInsertAfterHandle(@event, bytes, mqHashKey: mqHashKey);
                        RaiseEventAfter(@event, bytes);
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                this.GetLogger("Event_Raise").Log(LogCodes.EventRaiseError, Orleans.Runtime.Severity.Error, $"applay event {@event.TypeCode} error, eventId={@event.Version}", null, ex);
                throw ex;
            }
            return false;
        }
        private async Task EventInsertAfterHandle(IEventBase<K> @event, byte[] bytes, int recursion = 0, string mqHashKey = null)
        {
            try
            {
                if (SendEventToMQ)
                {
                    if (string.IsNullOrEmpty(mqHashKey)) mqHashKey = GrainId.ToString();
                    //消息写入消息队列                  
                    await MQService.Send(@event, bytes, mqHashKey);
                }
                //更改消息状态
                await EventStorage.Complete(@event);
                //保存快照
                await SaveSnapshotAsync();
            }
            catch (Exception e)
            {
                if (recursion > 5) throw e;
                this.GetLogger("Event_Raise").Log(LogCodes.EventCompleteError, Orleans.Runtime.Severity.Error, "事件complate操作出现致命异常:" + string.Format("Grain类型={0},GrainId={1},StateId={2},Version={3},错误信息:{4}", ThisType.FullName, GrainId, @event.StateId, @event.Version, e.Message), null, e);
                int newRecursion = recursion + 1;
                await Task.Delay(newRecursion * 200);
                await EventInsertAfterHandle(@event, bytes, newRecursion, mqHashKey: mqHashKey);
            }
        }
        /// <summary>
        /// 发送无状态更改的消息到消息队列
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public async Task SendMsg(IActorOwnMessage<K> msg)
        {
            msg.StateId = this.GrainId;
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, msg);
                await MQService.Send(msg, ms.ToArray(), GrainId.ToString());
            }
        }
        #endregion
    }
}
