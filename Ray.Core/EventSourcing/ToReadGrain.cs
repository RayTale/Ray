using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using Orleans;
using Ray.Core.Message;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.EventSourcing
{
    public abstract class ToReadGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : MessageWrapper
    {
        protected S State
        {
            get;
            set;
        }
        protected abstract K GrainId { get; }
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
        IEventStorage<K> _eventStorage;
        protected IEventStorage<K> EventStorage
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
        protected List<string> outsideMsgTypecodeList = new List<string>();
        protected void DeclareOutsideMsg(string typeCode)
        {
            outsideMsgTypecodeList.Add(typeCode);
        }
        public Task Tell(byte[] bytes)
        {
            using (var wms = new MemoryStream(bytes))
            {
                var message = Serializer.Deserialize<W>(wms);
                return Tell(message);
            }
        }
        public async Task Tell(W msg)
        {
            var type = MessageTypeMapping.GetType(msg.TypeCode);
            if (type != null)
            {
                using (var ems = new MemoryStream(msg.BinaryBytes))
                {
                    var message = Serializer.Deserialize(type, ems);
                    if (message != null)
                    {
                        if (!outsideMsgTypecodeList.Contains(msg.TypeCode))
                        {
                            if (message is IEvent @event)
                            {
                                if (@event.Version == this.State.Version + 1)
                                {
                                    await EventExcute(@event);
                                }
                                else if (@event.Version > this.State.Version)
                                {
                                    while (true)
                                    {
                                        var eventList = await EventStorage.GetListAsync(this.GrainId, this.State.Version, this.State.Version + 1000, this.State.VersionTime);
                                        foreach (var item in eventList)
                                        {
                                            await EventExcute(item.Event);
                                        }
                                        if (this.State.Version >= @event.Version) break;
                                    };
                                }
                            }
                        }
                        else if (message is IMessage value)
                        {
                            await NotEventMsgExcute(value);
                        }
                    }
                }
            }
        }
        protected Task UpdateState(IEvent @event)
        {
            this.State.Version = @event.Version;
            this.State.VersionTime = @event.Timestamp;
            if (@event.Version % 100 == 0)
            {
                return SaveSnapshotAsync();
            }
            else
                return Task.CompletedTask;
        }
        protected abstract bool ExecureExceptionFilter(Exception exception);
        protected Task EventExcute(IEvent @event)
        {
            return Execute(@event).ContinueWith(t =>
            {
                if (t.Exception == null)
                {
                    return UpdateState(@event);
                }
                else
                {
                    if (ExecureExceptionFilter(t.Exception.InnerException))
                    {
                        return UpdateState(@event);
                    }
                    throw t.Exception;
                }
            });
        }
        protected Task NotEventMsgExcute(IMessage msg)
        {
            return Execute(msg).ContinueWith(t =>
            {
                if (!ExecureExceptionFilter(t.Exception.InnerException))
                {
                    throw t.Exception;
                }
            });
        }
        protected virtual Task Execute(IMessage msg)
        {
            return Task.CompletedTask;
        }
        #region LifeTime
        public override async Task OnActivateAsync()
        {
            this.State = await StateStore.GetByIdAsync(GrainId);
            if (this.State == null)
            {
                IsNew = true;
                await InitState();
            }
        }
        #endregion
        #region State storage
        protected bool IsNew = false;
        protected async Task SaveSnapshotAsync()
        {
            if (IsNew)
            {
                await StateStore.InsertAsync(this.State);
                IsNew = false;
            }
            else
            {
                await StateStore.UpdateAsync(this.State);
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
        IStateStorage<S, K> _StateStore;
        private IStateStorage<S, K> StateStore
        {
            get
            {
                if (_StateStore == null)
                {
                    _StateStore = ServiceProvider.GetService<IStorageContainer>().GetReadStateStorage<K, S>(ThisType, this);
                }
                return _StateStore;
            }
        }
        #endregion
    }
}
