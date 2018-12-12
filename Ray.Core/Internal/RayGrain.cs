using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Messaging;
using Ray.Core.EventBus;
using Ray.Core.Utils;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Ray.Core.Internal
{
    public abstract class RayGrain<K, S, W> : Grain
        where S : class, IState<K>, new()
        where W : IMessageWrapper, new()
    {
        public RayGrain(ILogger logger)
        {
            Logger = logger;
        }
        protected ILogger Logger { get; private set; }
        protected IProducerContainer ProducerContainer { get; private set; }
        protected IStorageContainer StorageContainer { get; private set; }
        protected IJsonSerializer JsonSerializer { get; private set; }
        protected ISerializer Serializer { get; private set; }
        protected S State { get; set; }
        public abstract K GrainId { get; }
        protected virtual StateSnapSaveType SnapshotType => StateSnapSaveType.Master;
        protected virtual int SnapshotFrequency => 500;
        protected virtual int EventNumberPerRead => 2000;
        protected long StateStorageVersion { get; set; }
        protected virtual int SnapshotMinFrequency => 1;
        protected virtual bool SupportAsync => true;
        #region LifeTime
        public override Task OnActivateAsync()
        {
            StorageContainer = ServiceProvider.GetService<IStorageContainer>();
            ProducerContainer = ServiceProvider.GetService<IProducerContainer>();
            Serializer = ServiceProvider.GetService<ISerializer>();
            JsonSerializer = ServiceProvider.GetService<IJsonSerializer>();

            return RecoveryState();
        }
        protected virtual async Task RecoveryState()
        {
            var readSnapshotTask = ReadSnapshotAsync();
            if (!readSnapshotTask.IsCompleted)
                await readSnapshotTask;
            var eventStorageTask = GetEventStorage();
            if (!eventStorageTask.IsCompleted)
                await eventStorageTask;
            while (true)
            {
                var eventList = await eventStorageTask.Result.GetListAsync(GrainId, State.Version, State.Version + EventNumberPerRead, State.VersionTime);
                foreach (var @event in eventList)
                {
                    State.IncrementDoingVersion();//标记将要处理的Version
                    Apply(State, @event);
                    State.UpdateVersion(@event);//更新处理完成的Version
                }
                if (eventList.Count < EventNumberPerRead) break;
            };
        }
        public override Task OnDeactivateAsync()
        {
            if (State.Version - StateStorageVersion >= SnapshotMinFrequency)
                return SaveSnapshotAsync(true).AsTask();
            else
                return Task.CompletedTask;
        }
        #endregion
        #region State storage
        protected bool IsNew { get; set; } = false;
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

        protected virtual async ValueTask SaveSnapshotAsync(bool force = false)
        {
            if (SnapshotType == StateSnapSaveType.Master)
            {
                //如果版本号差超过设置则更新快照
                if (force || (State.Version - StateStorageVersion >= SnapshotFrequency))
                {
                    var onSaveSnapshotTask = OnSaveSnapshot();
                    if (!onSaveSnapshotTask.IsCompleted)
                        await onSaveSnapshotTask;
                    var getStateStorageTask = GetStateStorage();
                    if (!getStateStorageTask.IsCompleted)
                        await getStateStorageTask;
                    if (IsNew)
                    {
                        await getStateStorageTask.Result.InsertAsync(State);

                        StateStorageVersion = State.Version;
                        IsNew = false;
                    }
                    else
                    {
                        await getStateStorageTask.Result.UpdateAsync(State);
                        StateStorageVersion = State.Version;
                    }
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSaveSnapshot() => new ValueTask(Task.CompletedTask);
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
        protected async Task ClearStateAsync()
        {
            var getStateStorageTask = GetStateStorage();
            if (!getStateStorageTask.IsCompleted)
                await getStateStorageTask;
            await getStateStorageTask.Result.DeleteAsync(GrainId);
        }

        protected virtual ValueTask<IStateStorage<S, K>> GetStateStorage()
        {
            return StorageContainer.GetStateStorage<K, S>(this);
        }
        #endregion

        #region Event
        protected virtual ValueTask<IEventStorage<K>> GetEventStorage()
        {
            return StorageContainer.GetEventStorage<K, S>(this);
        }
        protected virtual ValueTask<IProducer> GetMQService()
        {
            return ProducerContainer.GetProducer(this);
        }
        protected virtual async Task<bool> RaiseEvent(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            try
            {
                State.IncrementDoingVersion();//标记将要处理的Version
                @event.StateId = GrainId;
                @event.Version = State.Version + 1;
                @event.Timestamp = DateTime.UtcNow;
                using (var ms = new PooledMemoryStream())
                {
                    Serializer.Serialize(ms, @event);
                    var bytes = ms.ToArray();
                    var getEventStorageTask = GetEventStorage();
                    if (!getEventStorageTask.IsCompleted)
                        await getEventStorageTask;
                    if (await getEventStorageTask.Result.SaveAsync(@event, bytes, uniqueId))
                    {
                        if (SupportAsync)
                        {
                            var data = new W
                            {
                                TypeName = @event.GetType().FullName,
                                Bytes = bytes
                            };
                            ms.Position = 0;
                            ms.SetLength(0);
                            Serializer.Serialize(ms, data);
                            //消息写入消息队列，以提供异步服务
                            Apply(State, @event);
                            var mqServiceTask = GetMQService();
                            if (!mqServiceTask.IsCompleted)
                                await mqServiceTask;
                            var publishTask = mqServiceTask.Result.Publish(ms.ToArray(), string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey);
                            if (!publishTask.IsCompleted)
                                await publishTask;
                        }
                        else
                        {
                            Apply(State, @event);
                        }
                        State.UpdateVersion(@event);//更新处理完成的Version
                        var saveSnapshotTask = SaveSnapshotAsync();
                        if (!saveSnapshotTask.IsCompleted)
                            await saveSnapshotTask;
                        OnRaiseSuccess(@event, bytes);
                        return true;
                    }
                    else
                        State.DecrementDoingVersion();//还原doing Version
                }
            }
            catch (Exception ex)
            {
                await RecoveryState();//还原状态
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
            return false;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual void OnRaiseSuccess(IEventBase<K> @event, byte[] bytes) { }
        protected abstract void Apply(S state, IEventBase<K> evt);
        /// <summary>
        /// 发送无状态更改的消息到消息队列
        /// </summary>
        /// <returns></returns>
        protected async ValueTask Publish<T>(T msg, string hashKey = null)
        {
            if (string.IsNullOrEmpty(hashKey))
                hashKey = GrainId.ToString();
            using (var ms = new PooledMemoryStream())
            {
                Serializer.Serialize(ms, msg);
                var data = new W
                {
                    TypeName = msg.GetType().FullName,
                    Bytes = ms.ToArray()
                };
                ms.Position = 0;
                ms.SetLength(0);
                Serializer.Serialize(ms, data);
                var mqServiceTask = GetMQService();
                if (!mqServiceTask.IsCompleted)
                    await mqServiceTask;
                var pubLishTask = mqServiceTask.Result.Publish(ms.ToArray(), hashKey);
                if (!pubLishTask.IsCompleted)
                    await pubLishTask;
            }
        }
        #endregion
    }
}
