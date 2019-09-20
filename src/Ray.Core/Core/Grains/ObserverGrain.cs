using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Channels;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using Ray.Core.Utils.Emit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Ray.Core
{
    public abstract class ObserverGrain<MainGrain, PrimaryKey> : Grain
    {
        readonly Func<object, IEvent, EventBase, Task> handlerInvokeFunc;
        readonly HandlerAttribute handlerAttribute;
        readonly bool concurrent;
        public ObserverGrain()
        {
            GrainType = GetType();
            concurrent = typeof(IConcurrentObserver).IsAssignableFrom(GrainType);
            var handlerAttributes = GrainType.GetCustomAttributes(typeof(HandlerAttribute), false);
            if (handlerAttributes.Length > 0)
                handlerAttribute = (HandlerAttribute)handlerAttributes[0];
            else
                handlerAttribute = default;
            var methods = GetType().GetMethods().Where(m =>
            {
                var parameters = m.GetParameters();
                return parameters.Length >= 1 && parameters.Any(p => typeof(IEvent).IsAssignableFrom(p.ParameterType));
            }).ToList();
            var dynamicMethod = new DynamicMethod($"Handler_Invoke", typeof(Task), new Type[] { typeof(object), typeof(IEvent), typeof(EventBase) }, GrainType, true);
            var ilGen = dynamicMethod.GetILGenerator();
            var items = new List<SwitchMethodEmit>();
            for (int i = 0; i < methods.Count; i++)
            {
                var method = methods[i];
                var methodParams = method.GetParameters();
                var caseType = methodParams.Single(p => typeof(IEvent).IsAssignableFrom(p.ParameterType)).ParameterType;
                items.Add(new SwitchMethodEmit
                {
                    Mehod = method,
                    CaseType = caseType,
                    DeclareLocal = ilGen.DeclareLocal(caseType),
                    Lable = ilGen.DefineLabel(),
                    Parameters = methodParams,
                    Index = i
                });
            }
            var defaultLabel = ilGen.DefineLabel();
            var lastLable = ilGen.DefineLabel();
            var declare_1 = ilGen.DeclareLocal(typeof(Task));
            foreach (var item in items)
            {
                ilGen.Emit(OpCodes.Ldarg_1);
                ilGen.Emit(OpCodes.Isinst, item.CaseType);
                if (item.Index > 3)
                {
                    ilGen.Emit(OpCodes.Stloc_S, item.DeclareLocal);
                    ilGen.Emit(OpCodes.Ldloc_S, item.DeclareLocal);
                }
                else
                {
                    if (item.Index == 0)
                    {
                        ilGen.Emit(OpCodes.Stloc_0);
                        ilGen.Emit(OpCodes.Ldloc_0);
                    }
                    else if (item.Index == 1)
                    {
                        ilGen.Emit(OpCodes.Stloc_1);
                        ilGen.Emit(OpCodes.Ldloc_1);
                    }
                    else if (item.Index == 2)
                    {
                        ilGen.Emit(OpCodes.Stloc_2);
                        ilGen.Emit(OpCodes.Ldloc_2);
                    }
                    else
                    {
                        ilGen.Emit(OpCodes.Stloc_3);
                        ilGen.Emit(OpCodes.Ldloc_3);
                    }
                }

                ilGen.Emit(OpCodes.Brtrue_S, item.Lable);
            }
            ilGen.Emit(OpCodes.Br_S, defaultLabel);
            foreach (var item in items)
            {
                ilGen.MarkLabel(item.Lable);
                ilGen.Emit(OpCodes.Ldarg_0);
                //加载第一个参数
                if (item.Parameters[0].ParameterType == item.CaseType)
                    LdEventArgs(item, ilGen);
                else if (item.Parameters[0].ParameterType == typeof(EventBase))
                    ilGen.Emit(OpCodes.Ldarg_2);
                //加载第二个参数
                if (item.Parameters.Length == 2)
                {
                    if (item.Parameters[1].ParameterType == item.CaseType)
                        LdEventArgs(item, ilGen);
                    else if (item.Parameters[1].ParameterType == typeof(EventBase))
                        ilGen.Emit(OpCodes.Ldarg_2);
                }
                ilGen.Emit(OpCodes.Call, item.Mehod);
                ilGen.Emit(OpCodes.Stloc_S, declare_1);
                ilGen.Emit(OpCodes.Br_S, lastLable);
            }
            ilGen.MarkLabel(defaultLabel);
            ilGen.Emit(OpCodes.Ldarg_0);
            ilGen.Emit(OpCodes.Ldarg_1);
            ilGen.Emit(OpCodes.Call, GrainType.GetMethod(nameof(DefaultHandler)));
            ilGen.Emit(OpCodes.Stloc_S, declare_1);
            ilGen.Emit(OpCodes.Br_S, lastLable);
            //last
            ilGen.MarkLabel(lastLable);
            ilGen.Emit(OpCodes.Ldloc_S, declare_1);
            ilGen.Emit(OpCodes.Ret);
            handlerInvokeFunc = (Func<object, IEvent, EventBase, Task>)dynamicMethod.CreateDelegate(typeof(Func<object, IEvent, EventBase, Task>));
            //加载Event参数
            static void LdEventArgs(SwitchMethodEmit item, ILGenerator gen)
            {
                if (item.Index > 3)
                {
                    gen.Emit(OpCodes.Ldloc_S, item.DeclareLocal);
                }
                else
                {
                    if (item.Index == 0)
                    {
                        gen.Emit(OpCodes.Ldloc_0);
                    }
                    else if (item.Index == 1)
                    {
                        gen.Emit(OpCodes.Ldloc_1);
                    }
                    else if (item.Index == 2)
                    {
                        gen.Emit(OpCodes.Ldloc_2);
                    }
                    else
                    {
                        gen.Emit(OpCodes.Ldloc_3);
                    }
                }
            }
        }
        /// <summary>
        /// 未处理事件列表
        /// </summary>
        private List<IFullyEvent<PrimaryKey>> UnprocessedEventList { get; set; }
        /// <summary>
        /// 多生产者单消费者消息信道
        /// </summary>
        protected IMpscChannel<AsyncInputEvent<IFullyEvent<PrimaryKey>, bool>> ConcurrentChannel { get; private set; }
        protected new IGrainFactory GrainFactory { get; private set; }
        private PrimaryKey _GrainId;
        private bool _GrainIdAcquired = false;
        public PrimaryKey GrainId
        {
            get
            {
                if (!_GrainIdAcquired)
                {
                    var type = typeof(PrimaryKey);
                    if (type == typeof(long) && this.GetPrimaryKeyLong() is PrimaryKey longKey)
                        _GrainId = longKey;
                    else if (type == typeof(string) && this.GetPrimaryKeyString() is PrimaryKey stringKey)
                        _GrainId = stringKey;
                    else if (type == typeof(Guid) && this.GetPrimaryKey() is PrimaryKey guidKey)
                        _GrainId = guidKey;
                    else
                        throw new ArgumentOutOfRangeException(typeof(PrimaryKey).FullName);
                    _GrainIdAcquired = true;
                }
                return _GrainId;
            }
        }
        protected CoreOptions ConfigOptions { get; private set; }
        protected ILogger Logger { get; private set; }
        protected ISerializer Serializer { get; private set; }
        /// <summary>
        /// Memory state, restored by snapshot + Event play or replay
        /// </summary>
        protected ObserverSnapshot<PrimaryKey> Snapshot { get; set; }
        /// <summary>
        /// 是否需要保存快照
        /// </summary>
        protected virtual bool SaveSnapshot => true;
        /// <summary>
        /// 是否全量激活，true代表启动时会执行大于快照版本的所有事件,false代表更快的启动，后续有事件进入的时候再处理大于快照版本的事件
        /// </summary>
        protected virtual bool FullyActive => false;
        /// <summary>
        /// 快照的事件版本号
        /// </summary>
        protected long SnapshotEventVersion { get; set; }
        /// <summary>
        /// 是否开启事件并发处理
        /// </summary>
        protected virtual bool ConcurrentHandle => false;
        /// <summary>
        /// Grain的Type
        /// </summary>
        protected Type GrainType { get; }
        /// <summary>
        /// 事件存储器
        /// </summary>
        protected IEventStorage<PrimaryKey> EventStorage { get; private set; }
        /// <summary>
        /// 状态存储器
        /// </summary>
        protected IObserverSnapshotStorage<PrimaryKey> ObserverSnapshotStorage { get; private set; }
        #region 初始化数据
        /// <summary>
        /// 依赖注入统一方法
        /// </summary>
        protected async virtual ValueTask DependencyInjection()
        {
            ConfigOptions = ServiceProvider.GetOptionsByName<CoreOptions>(typeof(MainGrain).FullName);
            Serializer = ServiceProvider.GetService<ISerializer>();
            GrainFactory = ServiceProvider.GetService<IGrainFactory>();
            Logger = (ILogger)ServiceProvider.GetService(typeof(ILogger<>).MakeGenericType(GrainType));
            var configureBuilder = ServiceProvider.GetService<IConfigureBuilder<PrimaryKey, MainGrain>>();
            var storageConfigTask = configureBuilder.GetConfig(ServiceProvider, GrainId);
            if (!storageConfigTask.IsCompletedSuccessfully)
                await storageConfigTask;
            var storageFactory = ServiceProvider.GetService(configureBuilder.StorageFactory) as IStorageFactory;
            //创建事件存储器
            var eventStorageTask = storageFactory.CreateEventStorage(storageConfigTask.Result, GrainId);
            if (!eventStorageTask.IsCompletedSuccessfully)
                await eventStorageTask;
            EventStorage = eventStorageTask.Result;
            //创建状态存储器
            var followConfigTask = configureBuilder.GetObserverConfig(ServiceProvider, GrainType, GrainId);
            if (!followConfigTask.IsCompletedSuccessfully)
                await followConfigTask;
            var stateStorageTask = storageFactory.CreateObserverSnapshotStorage(followConfigTask.Result, GrainId);
            if (!stateStorageTask.IsCompletedSuccessfully)
                await stateStorageTask;
            ObserverSnapshotStorage = stateStorageTask.Result;
        }
        public override async Task OnActivateAsync()
        {
            var dITask = DependencyInjection();
            if (!dITask.IsCompletedSuccessfully)
                await dITask;
            if (concurrent)
            {
                ConcurrentChannel = ServiceProvider.GetService<IMpscChannel<AsyncInputEvent<IFullyEvent<PrimaryKey>, bool>>>().BindConsumer(ConcurrentTell);
                UnprocessedEventList = new List<IFullyEvent<PrimaryKey>>();
            }
            try
            {
                await ReadSnapshotAsync();
                if (FullyActive)
                {
                    while (true)
                    {
                        var eventList = await EventStorage.GetList(GrainId, Snapshot.StartTimestamp, Snapshot.Version + 1, Snapshot.Version + ConfigOptions.NumberOfEventsPerRead);
                        await UnsafeTell(eventList);
                        if (eventList.Count < ConfigOptions.NumberOfEventsPerRead) break;
                    };
                }
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Activation completed: {0}->{1}", GrainType.FullName, Serializer.Serialize(Snapshot));
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Activation failed: {0}->{1}", GrainType.FullName, GrainId.ToString());
                throw;
            }
        }
        public override Task OnDeactivateAsync()
        {
            var needSaveSnap = Snapshot.Version - SnapshotEventVersion >= 1;
            if (concurrent)
                ConcurrentChannel.Complete();
            if (needSaveSnap)
            {
                var saveTask = SaveSnapshotAsync(true);
                if (!saveTask.IsCompletedSuccessfully)
                    return saveTask.AsTask();
            }
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Deactivate completed: {0}->{1}", GrainType.FullName, Serializer.Serialize(Snapshot));
            return Task.CompletedTask;
        }
        protected virtual async Task UnsafeTell(IEnumerable<IFullyEvent<PrimaryKey>> eventList)
        {
            if (ConcurrentHandle)
            {
                await Task.WhenAll(eventList.Select(@event =>
                {
                    var task = EventDelivered(@event);
                    if (!task.IsCompletedSuccessfully)
                        return task.AsTask();
                    else
                        return Task.CompletedTask;
                }));
                var lastEvt = eventList.Last();
                Snapshot.UnsafeUpdateVersion(lastEvt.Base);
            }
            else
            {
                foreach (var @event in eventList)
                {
                    Snapshot.IncrementDoingVersion(GrainType);//标记将要处理的Version
                    var task = EventDelivered(@event);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    Snapshot.UpdateVersion(@event.Base, GrainType);//更新处理完成的Version
                }
            }
            var saveTask = SaveSnapshotAsync();
            if (!saveTask.IsCompletedSuccessfully)
                await saveTask;
        }
        protected virtual async Task ReadSnapshotAsync()
        {
            try
            {
                Snapshot = await ObserverSnapshotStorage.Get(GrainId);
                if (Snapshot == null)
                {
                    var createTask = InitFirstSnapshot();
                    if (!createTask.IsCompletedSuccessfully)
                        await createTask;
                }
                SnapshotEventVersion = Snapshot.Version;
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("ReadSnapshot completed: {0}->{1}", GrainType.FullName, Serializer.Serialize(Snapshot));
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "ReadSnapshot failed: {0}->{1}", GrainType.FullName, GrainId.ToString());
                throw;
            }
        }
        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask InitFirstSnapshot()
        {
            Snapshot = new ObserverSnapshot<PrimaryKey>
            {
                StateId = GrainId
            };
            return Consts.ValueTaskDone;
        }
        #endregion
        public async Task OnNext(Immutable<byte[]> bytes)
        {
            var (success, transport) = EventBytesTransport.FromBytesWithNoId(bytes.Value);
            if (success)
            {
                var msgType = TypeContainer.GetType(transport.EventTypeCode);
                var data = Serializer.Deserialize(transport.EventBytes, msgType);
                if (data is IEvent @event)
                {
                    var eventBase = EventBase.FromBytes(transport.BaseBytes);
                    if (eventBase.Version > Snapshot.Version)
                    {
                        if (concurrent)
                        {
                            var input = new AsyncInputEvent<IFullyEvent<PrimaryKey>, bool>(new FullyEvent<PrimaryKey>
                            {
                                StateId = GrainId,
                                Base = eventBase,
                                Event = @event
                            });
                            var writeTask = ConcurrentChannel.WriteAsync(input);
                            if (!writeTask.IsCompletedSuccessfully)
                                await writeTask;
                            if (!writeTask.Result)
                            {
                                var ex = new ChannelUnavailabilityException(GrainId.ToString(), GrainType);
                                Logger.LogError(ex, ex.Message);
                                throw ex;
                            }
                            await input.TaskSource.Task;
                        }
                        else
                        {
                            var tellTask = Tell(new FullyEvent<PrimaryKey>
                            {
                                StateId = GrainId,
                                Base = eventBase,
                                Event = @event
                            });
                            if (!tellTask.IsCompletedSuccessfully)
                                await tellTask;
                        }
                    }
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogInformation("OnNext completed: {0}->{1}->{2}", GrainType.FullName, GrainId.ToString(), Serializer.Serialize(data, msgType));
                }
                else
                {
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogInformation("Non-Event: {0}->{1}->{2}", GrainType.FullName, GrainId.ToString(), Serializer.Serialize(data, msgType));
                }
            }
        }
        public Task<long> GetVersion()
        {
            return Task.FromResult(Snapshot.Version);
        }
        public async Task<long> GetAndSaveVersion(long compareVersion)
        {
            if (SnapshotEventVersion < compareVersion && Snapshot.Version >= compareVersion)
            {
                var saveTask = SaveSnapshotAsync(true);
                if (!saveTask.IsCompletedSuccessfully)
                    await saveTask;
            }
            return Snapshot.Version;
        }
        protected async ValueTask Tell(IFullyEvent<PrimaryKey> fullyEvent)
        {
            try
            {
                if (fullyEvent.Base.Version == Snapshot.Version + 1)
                {
                    var task = EventDelivered(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    Snapshot.FullUpdateVersion(fullyEvent.Base, GrainType);//更新处理完成的Version
                }
                else if (fullyEvent.Base.Version > Snapshot.Version)
                {
                    var eventList = await EventStorage.GetList(GrainId, Snapshot.StartTimestamp, Snapshot.Version + 1, fullyEvent.Base.Version - 1);
                    foreach (var evt in eventList)
                    {
                        var task = EventDelivered(evt);
                        if (!task.IsCompletedSuccessfully)
                            await task;
                        Snapshot.FullUpdateVersion(evt.Base, GrainType);//更新处理完成的Version
                    }
                }
                if (fullyEvent.Base.Version == Snapshot.Version + 1)
                {
                    var task = EventDelivered(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    Snapshot.FullUpdateVersion(fullyEvent.Base, GrainType);//更新处理完成的Version
                }
                if (fullyEvent.Base.Version > Snapshot.Version)
                {
                    throw new EventVersionUnorderedException(GrainId.ToString(), GrainType, fullyEvent.Base.Version, Snapshot.Version);
                }
                var saveTask = SaveSnapshotAsync();
                if (!saveTask.IsCompletedSuccessfully)
                    await saveTask;
            }
            catch
            {
                var saveTask = SaveSnapshotAsync(true);
                if (!saveTask.IsCompletedSuccessfully)
                    await saveTask;
                throw;
            }
        }
        private async Task ConcurrentTell(List<AsyncInputEvent<IFullyEvent<PrimaryKey>, bool>> inputs)
        {
            var evtList = new List<IFullyEvent<PrimaryKey>>();
            var startVersion = Snapshot.Version;
            if (UnprocessedEventList.Count > 0)
            {
                startVersion = UnprocessedEventList.Last().Base.Version;
            }
            var maxVersion = startVersion;
            TaskCompletionSource<bool> maxRequest = default;
            try
            {
                foreach (var input in inputs)
                {
                    if (input.Value.Base.Version == startVersion)
                    {
                        maxRequest = input.TaskSource;
                    }
                    else if (input.Value.Base.Version < startVersion)
                    {
                        input.TaskSource.TrySetResult(true);
                    }
                    else
                    {
                        evtList.Add(input.Value);
                        if (input.Value.Base.Version > maxVersion)
                        {
                            maxRequest?.TrySetResult(true);
                            maxVersion = input.Value.Base.Version;
                            maxRequest = input.TaskSource;
                        }
                        else
                        {
                            input.TaskSource.TrySetResult(true);
                        }
                    }
                }

                if (evtList.Count > 0)
                {
                    var orderList = evtList.OrderBy(e => e.Base.Version).ToList();
                    var inputLast = orderList.Last();
                    if (startVersion + orderList.Count < inputLast.Base.Version)
                    {
                        var loadList = await EventStorage.GetList(GrainId, 0, startVersion + 1, inputLast.Base.Version - 1);
                        UnprocessedEventList.AddRange(loadList);
                        UnprocessedEventList.Add(inputLast);
                    }
                    else
                    {
                        UnprocessedEventList.AddRange(orderList.Select(w => w));
                    }
                }
                if (UnprocessedEventList.Count > 0)
                {
                    await Task.WhenAll(UnprocessedEventList.Select(async @event =>
                    {
                        var task = EventDelivered(@event);
                        if (!task.IsCompletedSuccessfully)
                            await task;
                    }));
                    Snapshot.UnsafeUpdateVersion(UnprocessedEventList.Last().Base);
                    var saveTask = SaveSnapshotAsync();
                    if (!saveTask.IsCompletedSuccessfully)
                        await saveTask;
                    UnprocessedEventList.Clear();
                    maxRequest?.TrySetResult(true);
                }
            }
            catch (Exception ex)
            {
                maxRequest?.TrySetException(ex);
            }
        }
        protected virtual async ValueTask EventDelivered(IFullyEvent<PrimaryKey> fullyEvent)
        {
            try
            {
                if (SnapshotEventVersion > 0 &&
                    Snapshot.Version > 0 &&
                    fullyEvent.Base.Timestamp < Snapshot.StartTimestamp)
                {
                    await ObserverSnapshotStorage.UpdateStartTimestamp(Snapshot.StateId, fullyEvent.Base.Timestamp);
                }
                var task = OnEventDelivered(fullyEvent);
                if (!task.IsCompletedSuccessfully)
                    await task;
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Delivered failed: {0}->{1}->{2}", GrainType.FullName, GrainId.ToString(), Serializer.Serialize(fullyEvent, fullyEvent.GetType()));
            }
        }
        public Task DefaultHandler(IEvent evt)
        {
            if (handlerAttribute == default || !handlerAttribute.Ignores.Contains(evt.GetType()))
            {
                throw new UnfindEventHandlerException(evt.GetType());
            }
            return Task.CompletedTask;
        }
        protected virtual ValueTask OnEventDelivered(IFullyEvent<PrimaryKey> fullyEvent)
        {
            return new ValueTask(handlerInvokeFunc(this, fullyEvent.Event, fullyEvent.Base));
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSaveSnapshot() => Consts.ValueTaskDone;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSavedSnapshot() => new ValueTask();
        protected virtual async ValueTask SaveSnapshotAsync(bool force = false)
        {
            if (SaveSnapshot)
            {
                if ((force && Snapshot.Version > SnapshotEventVersion) ||
                    (Snapshot.Version - SnapshotEventVersion >= ConfigOptions.ObserverSnapshotVersionInterval))
                {
                    try
                    {
                        var onSaveSnapshotTask = OnSaveSnapshot();//自定义保存项
                        if (!onSaveSnapshotTask.IsCompletedSuccessfully)
                            await onSaveSnapshotTask;
                        if (SnapshotEventVersion == 0)
                        {
                            await ObserverSnapshotStorage.Insert(Snapshot);
                        }
                        else
                        {
                            await ObserverSnapshotStorage.Update(Snapshot);
                        }
                        SnapshotEventVersion = Snapshot.Version;
                        var onSavedSnapshotTask = OnSavedSnapshot();
                        if (!onSavedSnapshotTask.IsCompletedSuccessfully)
                            await onSavedSnapshotTask;
                        if (Logger.IsEnabled(LogLevel.Trace))
                            Logger.LogTrace("SaveSnapshot completed: {0}->{1}", GrainType.FullName, Serializer.Serialize(Snapshot));
                    }
                    catch (Exception ex)
                    {
                        Logger.LogCritical(ex, "SaveSnapshot failed: {0}->{1}", GrainType.FullName, GrainId.ToString());
                        throw;
                    }
                }
            }
        }
    }
}
