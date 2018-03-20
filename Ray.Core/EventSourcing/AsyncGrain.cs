using Ray.Core.Message;
using Orleans;
using System;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using System.Runtime.CompilerServices;

namespace Ray.Core.EventSourcing
{
    public abstract class AsyncGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : MessageWrapper
    {
        protected S State
        {
            get;
            set;
        }
        protected abstract K GrainId { get; }
        protected virtual bool SaveSnapshot => true;
        protected virtual int SnapshotFrequency => 500;
        protected Int64 StateStorageVersion { get; set; }
        protected virtual int SnapshotMinFrequency => 1;
        IEventStorage<K> _eventStorage;
        protected IEventStorage<K> EventStorage
        {
            get
            {
                if (_eventStorage == null)
                {
                    _eventStorage = ServiceProvider.GetService<IStorageContainer>().GetEventStorage<K, S>(GetType(), this);
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
                    _StateStore = ServiceProvider.GetService<IStorageContainer>().GetStateStorage<K, S>(GetType(), this);
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
                    if (Serializer.Deserialize(type, ems) is IEventBase<K> @event)
                    {
                        if (@event.Version == State.Version + 1)
                        {
                            State.IncrementDoingVersion();//标记将要处理的Version
                            try
                            {
                                await OnEventDelivered(@event);
                                State.UpdateVersion(@event);//更新处理完成的Version
                            }
                            catch (Exception e)
                            {
                                State.DoingVersion = State.Version;//标记将要处理的Version
                                throw e;
                            }
                            await OnExecuted(@event);
                            await SaveSnapshotAsync();
                        }
                        else if (@event.Version > State.Version)
                        {
                            var eventList = await EventStorage.GetListAsync(GrainId, State.Version, @event.Version);
                            foreach (var item in eventList)
                            {
                                State.IncrementDoingVersion();//标记将要处理的Version
                                try
                                {
                                    await OnEventDelivered(item);
                                    State.UpdateVersion(item);//更新处理完成的Version
                                }
                                catch (Exception e)
                                {
                                    State.DoingVersion = State.Version;//标记将要处理的Version
                                    throw e;
                                }
                                await OnExecuted(@event);
                                await SaveSnapshotAsync();
                            }
                        }
                        if (@event.Version == State.Version + 1)
                        {
                            State.IncrementDoingVersion();//标记将要处理的Version
                            try
                            {
                                await OnEventDelivered(@event);
                                State.UpdateVersion(@event);//更新处理完成的Version
                            }
                            catch (Exception e)
                            {
                                State.DoingVersion = State.Version;//标记将要处理的Version
                                throw e;
                            }
                            await OnExecuted(@event);
                            await SaveSnapshotAsync();
                        }
                        if (@event.Version > State.Version)
                        {
                            throw new Exception($"Event version of the error,Type={GetType().FullName},StateId={this.GrainId.ToString()},StateVersion={State.Version},EventVersion={@event.Version}");
                        }
                    }
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task OnEventDelivered(IEventBase<K> @event)
        {
            return Task.CompletedTask;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task OnExecuted(IEventBase<K> @event)
        {
            return Task.CompletedTask;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task OnSaveSnapshot()
        {
            return Task.CompletedTask;
        }
        protected virtual async Task SaveSnapshotAsync(bool force = false)
        {
            if (SaveSnapshot)
            {
                if (force || (State.Version - StateStorageVersion >= SnapshotFrequency))
                {
                    await OnSaveSnapshot();//自定义保存项
                    if (IsNew)
                    {
                        await StateStore.InsertAsync(State);
                        IsNew = false;
                    }
                    else
                    {
                        await StateStore.UpdateAsync(State);
                    }
                    StateStorageVersion = State.Version;
                }
            }
        }
        #region 初始化数据
        public override async Task OnActivateAsync()
        {
            await ReadSnapshotAsync();
            var storageContainer = ServiceProvider.GetService<IStorageContainer>();
            while (true)
            {
                var eventList = await EventStorage.GetListAsync(GrainId, State.Version, State.Version + 1000, State.VersionTime);
                foreach (var @event in eventList)
                {
                    State.IncrementDoingVersion();//标记将要处理的Version
                    await OnEventDelivered(@event);
                    State.UpdateVersion(@event);//更新处理完成的Version
                }
                if (eventList.Count < 1000) break;
            };
        }
        public override Task OnDeactivateAsync()
        {
            if (State.Version - StateStorageVersion >= SnapshotMinFrequency)
                return SaveSnapshotAsync(true);
            else
                return Task.CompletedTask;
        }
        protected bool IsNew { get; set; }
        protected virtual async Task ReadSnapshotAsync()
        {
            State = await StateStore.GetByIdAsync(GrainId);
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
    }
}
