using Ray.Core.Message;
using Orleans;
using System;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.EventSourcing
{
    public abstract class ESRepGrain<K, S, W> : Grain
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
        protected virtual SnapshotType SnapshotType { get { return SnapshotType.Replica; } }
        protected virtual int SnapshotFrequency { get { return 50; } }
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
            var type = MessageTypeMapper.GetType(message.TypeCode);
            if (type != null)
            {
                using (var ems = new MemoryStream(message.BinaryBytes))
                {
                    var @event = Serializer.Deserialize(type, ems) as IEventBase<K>;
                    if (@event != null)
                    {
                        if (@event.Version == this.State.Version + 1)
                        {
                            await OnExecution(@event);
                            this.State.IncrementDoingVersion();//标记将要处理的Version
                            try
                            {
                                EventHandle.Apply(this.State, @event);
                                this.State.UpdateVersion(@event);//更新处理完成的Version
                            }
                            catch (Exception e)
                            {
                                this.State.DoingVersion = this.State.Version;//标记将要处理的Version
                                throw e;
                            }
                            await OnExecuted(@event);
                            await SaveSnapshotAsync();
                        }
                        else if (@event.Version > this.State.Version)
                        {
                            var eventList = await EventStorage.GetListAsync(this.GrainId, this.State.Version, @event.Version);
                            foreach (var item in eventList)
                            {
                                await OnExecution(item.Event);
                                this.State.IncrementDoingVersion();//标记将要处理的Version
                                try
                                {
                                    EventHandle.Apply(this.State, item.Event);
                                    this.State.UpdateVersion(item.Event);//更新处理完成的Version
                                }
                                catch (Exception e)
                                {
                                    this.State.DoingVersion = this.State.Version;//标记将要处理的Version
                                    throw e;
                                }
                                await OnExecuted(item.Event);
                                await SaveSnapshotAsync();
                            }
                        }
                        if (@event.Version == this.State.Version + 1)
                        {
                            await OnExecution(@event);
                            this.State.IncrementDoingVersion();//标记将要处理的Version
                            try
                            {
                                EventHandle.Apply(this.State, @event);
                                this.State.UpdateVersion(@event);//更新处理完成的Version
                            }
                            catch (Exception e)
                            {
                                this.State.DoingVersion = this.State.Version;//标记将要处理的Version
                                throw e;
                            }
                            await OnExecuted(@event);
                            await SaveSnapshotAsync();
                        }
                        if (@event.Version > this.State.Version)
                        {
                            throw new Exception($"Event version of the error,Type={ThisType.FullName},StateId={this.GrainId.ToString()},StateVersion={this.State.Version},EventVersion={@event.Version}");
                        }
                    }
                }
            }
        }
        protected virtual Task OnExecuted(IEventBase<K> @event)
        {
            return Task.CompletedTask;
        }
        protected virtual Task OnExecution(IEventBase<K> @event)
        {
            return Task.CompletedTask;
        }
        public override Task OnActivateAsync()
        {
            return Active();
        }
        protected virtual Task CustomSave()
        {
            return Task.CompletedTask;
        }
        protected virtual async Task SaveSnapshotAsync()
        {
            if (SnapshotType == SnapshotType.Replica)
            {
                if (this.State.Version - storageVersion >= SnapshotFrequency)
                {
                    await CustomSave();//自定义保存项
                    if (IsNew)
                    {
                        await StateStore.InsertAsync(this.State);
                        IsNew = false;
                    }
                    else
                    {
                        await StateStore.UpdateAsync(this.State);
                    }
                    storageVersion = this.State.Version;
                }
            }
        }
        #region 初始化数据
        private async Task Active()
        {
            await ReadSnapshotAsync();
            var storageContainer = ServiceProvider.GetService<IStorageContainer>();
            while (true)
            {
                var eventList = await EventStorage.GetListAsync(this.GrainId, this.State.Version, this.State.Version + 1000, this.State.VersionTime);
                foreach (var @event in eventList)
                {
                    this.State.IncrementDoingVersion();//标记将要处理的Version
                    EventHandle.Apply(this.State, @event.Event);
                    this.State.UpdateVersion(@event.Event);//更新处理完成的Version
                }
                if (eventList.Count < 1000) break;
            };
        }
        protected bool IsNew = false;
        protected virtual async Task ReadSnapshotAsync()
        {
            this.State = await StateStore.GetByIdAsync(GrainId);
            if (this.State == null)
            {
                IsNew = true;
                await InitState();
            }
            storageVersion = this.State.Version;
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
        #endregion
    }
}
