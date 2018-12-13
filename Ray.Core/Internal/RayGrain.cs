using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.EventBus;
using Ray.Core.Messaging;
using Ray.Core.Utils;

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
        protected RayConfigOptions ConfigOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected IProducerContainer ProducerContainer { get; private set; }
        protected IStorageContainer StorageContainer { get; private set; }
        protected IJsonSerializer JsonSerializer { get; private set; }
        protected ISerializer Serializer { get; private set; }
        protected S State { get; set; }
        public abstract K GrainId { get; }
        protected virtual StateSnapStorageType SnapshotStorageType => StateSnapStorageType.Master;
        /// <summary>
        /// 保存快照的事件Version间隔
        /// </summary>
        protected virtual int SnapshotVersionInterval => ConfigOptions.SnapshotVersionInterval;
        /// <summary>
        /// 分批次批量读取事件的时候每次读取的数据量
        /// </summary>
        protected virtual int NumberOfEventsPerRead => ConfigOptions.NumberOfEventsPerRead;
        /// <summary>
        /// 快照的事件版本号
        /// </summary>
        protected long SnapshotEventVersion { get; set; }
        /// <summary>
        /// 失活的时候保存快照的最小事件Version间隔
        /// </summary>
        protected virtual int SnapshotMinVersionInterval => ConfigOptions.SnapshotMinVersionInterval;
        /// <summary>
        /// 是否支持异步follow，true代表事件会广播，false事件不会进行广播
        /// </summary>
        protected virtual bool SupportAsyncFollow => true;
        protected Type GrainType { get; private set; }
        #region LifeTime
        public override Task OnActivateAsync()
        {
            GrainType = GetType();
            ConfigOptions = ServiceProvider.GetService<IOptions<RayConfigOptions>>().Value;
            StorageContainer = ServiceProvider.GetService<IStorageContainer>();
            ProducerContainer = ServiceProvider.GetService<IProducerContainer>();
            Serializer = ServiceProvider.GetService<ISerializer>();
            JsonSerializer = ServiceProvider.GetService<IJsonSerializer>();

            if (Logger.IsEnabled(LogLevel.Information))
            {
                Logger.LogInformation("{0} with id {1} starts to activate", GrainType.FullName, GrainId.ToString());
            }
            return RecoveryState();
        }
        protected virtual async Task RecoveryState()
        {
            if (Logger.IsEnabled(LogLevel.Information))
            {
                Logger.LogInformation("Start repair state of {0} with ID {1}", GrainType.FullName, GrainId.ToString());
            }
            var readSnapshotTask = ReadSnapshotAsync();
            if (!readSnapshotTask.IsCompleted)
                await readSnapshotTask;
            var eventStorageTask = GetEventStorage();
            if (!eventStorageTask.IsCompleted)
                await eventStorageTask;
            while (true)
            {
                var eventList = await eventStorageTask.Result.GetListAsync(GrainId, State.Version, State.Version + NumberOfEventsPerRead, State.VersionTime);
                foreach (var @event in eventList)
                {
                    State.IncrementDoingVersion(GrainType);//标记将要处理的Version
                    Apply(State, @event);
                    State.UpdateVersion(@event, GrainType);//更新处理完成的Version
                }
                if (eventList.Count < NumberOfEventsPerRead) break;
            };
            if (Logger.IsEnabled(LogLevel.Information))
            {
                Logger.LogInformation("Repair state of {0} with ID {1} completed,state version is {2}", GrainType.FullName, GrainId.ToString(), State.Version);
            }
        }
        public override Task OnDeactivateAsync()
        {
            if (State.Version - SnapshotEventVersion >= SnapshotMinVersionInterval)
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
            if (State == default)
            {
                IsNew = true;
                var createTask = CreateState();
                if (!createTask.IsCompleted)
                    await createTask;
            }
            SnapshotEventVersion = State.Version;
        }

        protected virtual async ValueTask SaveSnapshotAsync(bool force = false)
        {
            if (SnapshotStorageType == StateSnapStorageType.Master)
            {
                //如果版本号差超过设置则更新快照
                if (force || (State.Version - SnapshotEventVersion >= SnapshotVersionInterval))
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

                        SnapshotEventVersion = State.Version;
                        IsNew = false;
                    }
                    else
                    {
                        await getStateStorageTask.Result.UpdateAsync(State);
                        SnapshotEventVersion = State.Version;
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
                State.IncrementDoingVersion(GrainType);//标记将要处理的Version
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
                        if (SupportAsyncFollow)
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
                        State.UpdateVersion(@event, GrainType);//更新处理完成的Version
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
