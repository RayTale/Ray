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
        protected long SnapshotEventVersion { get; private set; }
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
        /// <summary>
        /// Grain激活时调用用来初始化的方法(禁止在子类重写,请使用)
        /// </summary>
        /// <returns></returns>
        public override async Task OnActivateAsync()
        {
            try
            {
                GrainType = GetType();
                ConfigOptions = ServiceProvider.GetService<IOptions<RayConfigOptions>>().Value;
                StorageContainer = ServiceProvider.GetService<IStorageContainer>();
                ProducerContainer = ServiceProvider.GetService<IProducerContainer>();
                Serializer = ServiceProvider.GetService<ISerializer>();
                JsonSerializer = ServiceProvider.GetService<IJsonSerializer>();
                await RecoveryState();
                await OnBaseActivated();
                if (Logger.IsEnabled(LogLevel.Information))
                {
                    Logger.LogInformation(LogEventIds.GrainActivateId, "Grain activated,type {0} with id {1}", GrainType.FullName, GrainId.ToString());
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                {
                    Logger.LogError(LogEventIds.GrainActivateId, ex, "Grain activation failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                }
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task OnBaseActivated() => Task.CompletedTask;
        protected virtual async Task RecoveryState()
        {
            try
            {
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
                    Logger.LogInformation(LogEventIds.GrainStateRecoveryId, "State repair successfully,type {0} with Id {1} ,state version is {2}", GrainType.FullName, GrainId.ToString(), State.Version);
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                {
                    Logger.LogError(LogEventIds.GrainActivateId, ex, "Grain repair state failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                }
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
        public override async Task OnDeactivateAsync()
        {
            var needSaveSnap = State.Version - SnapshotEventVersion >= SnapshotMinVersionInterval;
            try
            {
                if (needSaveSnap)
                {
                    var saveTask = SaveSnapshotAsync(true);
                    if (!saveTask.IsCompleted)
                        await saveTask;
                    await OnBaseDeactivated();
                }
                if (Logger.IsEnabled(LogLevel.Information))
                {
                    Logger.LogInformation(LogEventIds.GrainDeactivateId, "Grain has been deactivated,type {0} with id {1} ,{}", GrainType.FullName, GrainId.ToString(), needSaveSnap ? "updated snapshot" : "no update snapshot");
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                {
                    Logger.LogError(LogEventIds.GrainActivateId, ex, "Grain Deactivate failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                }
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task OnBaseDeactivated() => Task.CompletedTask;
        #endregion
        #region State storage
        /// <summary>
        ///  true:当前状态无快照,false:当前状态已经存在快照
        /// </summary>
        protected bool NoSnapshot { get; private set; }
        protected virtual async Task ReadSnapshotAsync()
        {
            try
            {
                var stateStorageTask = GetStateStorage();
                if (!stateStorageTask.IsCompleted)
                    await stateStorageTask;
                State = await stateStorageTask.Result.GetByIdAsync(GrainId);
                if (State == default)
                {
                    NoSnapshot = true;
                    var createTask = CreateState();
                    if (!createTask.IsCompleted)
                        await createTask;
                }
                SnapshotEventVersion = State.Version;
                if (Logger.IsEnabled(LogLevel.Information))
                {
                    Logger.LogInformation(LogEventIds.GrainStateRecoveryId, "State snapshot read successfully, type {0} with Id {1}, state version is {2}", GrainType.FullName, GrainId.ToString(), State.Version);
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                {
                    Logger.LogError(LogEventIds.GrainStateRecoveryId, ex, "State snapshot read failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                }
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }

        protected async ValueTask SaveSnapshotAsync(bool force = false)
        {
            if (SnapshotStorageType == StateSnapStorageType.Master)
            {
                //如果版本号差超过设置则更新快照
                if (force || (State.Version - SnapshotEventVersion >= SnapshotVersionInterval))
                {
                    try
                    {
                        var onSaveSnapshotTask = OnSaveSnapshot();
                        if (!onSaveSnapshotTask.IsCompleted)
                            await onSaveSnapshotTask;
                        var getStateStorageTask = GetStateStorage();
                        if (!getStateStorageTask.IsCompleted)
                            await getStateStorageTask;
                        if (NoSnapshot)
                        {
                            await getStateStorageTask.Result.InsertAsync(State);

                            SnapshotEventVersion = State.Version;
                            NoSnapshot = false;
                        }
                        else
                        {
                            await getStateStorageTask.Result.UpdateAsync(State);
                            SnapshotEventVersion = State.Version;
                        }
                        if (Logger.IsEnabled(LogLevel.Information))
                        {
                            Logger.LogInformation(LogEventIds.GrainSaveSnapshot, "State snapshot saved successfully, type {0} with Id {1} ,state version is {2}", GrainType.FullName, GrainId.ToString(), State.Version);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsEnabled(LogLevel.Error))
                        {
                            Logger.LogError(LogEventIds.GrainSaveSnapshot, ex, "State snapshot save failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                        }
                        ExceptionDispatchInfo.Capture(ex).Throw();
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
        protected virtual ValueTask<IProducer> GetEventProducer()
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
                            var mqServiceTask = GetEventProducer();
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
                        if (Logger.IsEnabled(LogLevel.Information))
                        {
                            Logger.LogInformation(LogEventIds.GrainRaiseEvent, "Event raise successfully, type {0} with Id {1},state version is {2},event type {3} with version {4}", GrainType.FullName, GrainId.ToString(), State.Version, @event.GetType().FullName, @event.Version);
                        }
                        return true;
                    }
                    else
                    {
                        if (Logger.IsEnabled(LogLevel.Information))
                        {
                            Logger.LogInformation(LogEventIds.GrainRaiseEvent, "Event raise failure because of idempotency limitation, type {0} with Id {1},state version is {2},event type {3} with version {4}", GrainType.FullName, GrainId.ToString(), State.Version, @event.GetType().FullName, @event.Version);
                        }
                        State.DecrementDoingVersion();//还原doing Version
                    }
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                {
                    Logger.LogError(LogEventIds.GrainRaiseEvent, ex, "Event raise produces errors, type {0} with Id {1},state version is {2},event:{3}", GrainType.FullName, GrainId.ToString(), State.Version, JsonSerializer.Serialize(@event));
                }
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
                var mqServiceTask = GetEventProducer();
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
