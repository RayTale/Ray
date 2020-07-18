using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Abstractions;
using Ray.Core.Abstractions.Monitor;
using Ray.Core.Configuration;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Observer;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;
using Ray.Core.Utils.Emit;

namespace Ray.Core
{
    public abstract class ObserverGrain<PrimaryKey, MainGrain> : Grain, IObserver
    {
        private static readonly ConcurrentDictionary<Type, Func<object, IEvent, EventBasicInfo, FullyEvent<PrimaryKey>, string, Task>> grainHandlerDict = new ConcurrentDictionary<Type, Func<object, IEvent, EventBasicInfo, FullyEvent<PrimaryKey>, string, Task>>();
        private static readonly ConcurrentDictionary<Type, EventIgnoreAttribute> grainIgnoreEventDict = new ConcurrentDictionary<Type, EventIgnoreAttribute>();
        private readonly Func<object, IEvent, EventBasicInfo, FullyEvent<PrimaryKey>, string, Task> handlerInvokeFunc;
        private readonly EventIgnoreAttribute handlerAttribute;

        public ObserverGrain()
        {
            this.GrainType = this.GetType();
            this.handlerAttribute = grainIgnoreEventDict.GetOrAdd(this.GrainType, type =>
            {
                var handlerAttributes = this.GrainType.GetCustomAttributes(typeof(EventIgnoreAttribute), false);
                if (handlerAttributes.Length > 0)
                {
                    return (EventIgnoreAttribute)handlerAttributes[0];
                }
                else
                {
                    return default;
                }
            });

            this.handlerInvokeFunc = grainHandlerDict.GetOrAdd(this.GrainType, type =>
            {
                var methods = this.GetType().GetMethods().Where(m =>
                {
                    var parameters = m.GetParameters();
                    return parameters.Length >= 1 && parameters.Any(p => typeof(IEvent).IsAssignableFrom(p.ParameterType) && !p.ParameterType.IsInterface);
                }).ToList();
                var eventUIDMethod = typeof(CoreExtensions).GetMethod(nameof(CoreExtensions.GetNextUID)).MakeGenericMethod(typeof(PrimaryKey));
                var dynamicMethod = new DynamicMethod($"Handler_Invoke", typeof(Task), new Type[] { typeof(object), typeof(IEvent), typeof(EventBasicInfo), typeof(FullyEvent<PrimaryKey>), typeof(string) }, type, true);
                var ilGen = dynamicMethod.GetILGenerator();
                var switchMethods = new List<SwitchMethodEmit>();
                for (int i = 0; i < methods.Count; i++)
                {
                    var method = methods[i];
                    var methodParams = method.GetParameters();
                    var caseType = methodParams.Single(p => typeof(IEvent).IsAssignableFrom(p.ParameterType)).ParameterType;
                    switchMethods.Add(new SwitchMethodEmit
                    {
                        Mehod = method,
                        CaseType = caseType,
                        DeclareLocal = ilGen.DeclareLocal(caseType),
                        Lable = ilGen.DefineLabel(),
                        Parameters = methodParams,
                        Index = i
                    });
                }

                var sortList = new List<SwitchMethodEmit>();
                foreach (var item in switchMethods.Where(m => !typeof(IEvent).IsAssignableFrom(m.CaseType.BaseType)))
                {
                    sortList.Add(item);
                    GetInheritor(item, switchMethods, sortList);
                }

                sortList.Reverse();
                foreach (var item in switchMethods)
                {
                    if (!sortList.Contains(item))
                    {
                        sortList.Add(item);
                    }
                }

                var defaultLabel = ilGen.DefineLabel();
                var lastLable = ilGen.DefineLabel();
                var declare_1 = ilGen.DeclareLocal(typeof(Task));
                foreach (var item in sortList)
                {
                    ilGen.Emit(OpCodes.Ldarg_1);
                    ilGen.Emit(OpCodes.Isinst, item.CaseType);
                    if (item.Index > 3)
                    {
                        if (item.DeclareLocal.LocalIndex > 0 && item.DeclareLocal.LocalIndex <= 255)
                        {
                            ilGen.Emit(OpCodes.Stloc_S, item.DeclareLocal);
                            ilGen.Emit(OpCodes.Ldloc_S, item.DeclareLocal);
                        }
                        else
                        {
                            ilGen.Emit(OpCodes.Stloc, item.DeclareLocal);
                            ilGen.Emit(OpCodes.Ldloc, item.DeclareLocal);
                        }
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

                    ilGen.Emit(OpCodes.Brtrue, item.Lable);
                }

                ilGen.Emit(OpCodes.Br, defaultLabel);
                foreach (var item in sortList)
                {
                    ilGen.MarkLabel(item.Lable);
                    ilGen.Emit(OpCodes.Ldarg_0);
                    //加载第一个参数
                    if (item.Parameters[0].ParameterType == item.CaseType)
                    {
                        LdEventArgs(item, ilGen);
                    }
                    else if (item.Parameters[0].ParameterType == typeof(EventBasicInfo))
                    {
                        ilGen.Emit(OpCodes.Ldarg_2);
                    }
                    else if (item.Parameters[0].ParameterType == typeof(EventUID))
                    {
                        ilGen.Emit(OpCodes.Ldarg_3);
                        ilGen.Emit(OpCodes.Ldarg_S, 4);
                        ilGen.Emit(OpCodes.Call, eventUIDMethod);
                    }

                    //加载第二个参数
                    if (item.Parameters.Length >= 2)
                    {
                        if (item.Parameters[1].ParameterType == item.CaseType)
                        {
                            LdEventArgs(item, ilGen);
                        }
                        else if (item.Parameters[1].ParameterType == typeof(EventBasicInfo))
                        {
                            ilGen.Emit(OpCodes.Ldarg_2);
                        }
                        else if (item.Parameters[1].ParameterType == typeof(EventUID))
                        {
                            ilGen.Emit(OpCodes.Ldarg_3);
                            ilGen.Emit(OpCodes.Ldarg_S, 4);
                            ilGen.Emit(OpCodes.Call, eventUIDMethod);
                        }
                    }

                    //加载第三个参数
                    if (item.Parameters.Length >= 3)
                    {
                        if (item.Parameters[2].ParameterType == item.CaseType)
                        {
                            LdEventArgs(item, ilGen);
                        }
                        else if (item.Parameters[2].ParameterType == typeof(EventBasicInfo))
                        {
                            ilGen.Emit(OpCodes.Ldarg_2);
                        }
                        else if (item.Parameters[2].ParameterType == typeof(EventUID))
                        {
                            ilGen.Emit(OpCodes.Ldarg_3);
                            ilGen.Emit(OpCodes.Ldarg_S, 4);
                            ilGen.Emit(OpCodes.Call, eventUIDMethod);
                        }
                    }

                    ilGen.Emit(OpCodes.Call, item.Mehod);
                    if (item.DeclareLocal.LocalIndex > 0 && item.DeclareLocal.LocalIndex <= 255)
                    {
                        ilGen.Emit(OpCodes.Stloc_S, declare_1);
                    }
                    else
                    {
                        ilGen.Emit(OpCodes.Stloc, declare_1);
                    }

                    ilGen.Emit(OpCodes.Br, lastLable);
                }

                ilGen.MarkLabel(defaultLabel);
                ilGen.Emit(OpCodes.Ldarg_0);
                ilGen.Emit(OpCodes.Ldarg_1);
                ilGen.Emit(OpCodes.Call, type.GetMethod(nameof(this.DefaultHandler)));
                if (declare_1.LocalIndex > 0 && declare_1.LocalIndex <= 255)
                {
                    ilGen.Emit(OpCodes.Stloc_S, declare_1);
                }
                else
                {
                    ilGen.Emit(OpCodes.Stloc, declare_1);
                }

                ilGen.Emit(OpCodes.Br, lastLable);
                //last
                ilGen.MarkLabel(lastLable);
                if (declare_1.LocalIndex > 0 && declare_1.LocalIndex <= 255)
                {
                    ilGen.Emit(OpCodes.Ldloc_S, declare_1);
                }
                else
                {
                    ilGen.Emit(OpCodes.Ldloc, declare_1);
                }

                ilGen.Emit(OpCodes.Ret);
                var parames = new ParameterExpression[] { Expression.Parameter(typeof(object)), Expression.Parameter(typeof(IEvent)), Expression.Parameter(typeof(EventBasicInfo)), Expression.Parameter(typeof(FullyEvent<PrimaryKey>)), Expression.Parameter(typeof(string)) };
                var body = Expression.Call(dynamicMethod, parames);
                return Expression.Lambda<Func<object, IEvent, EventBasicInfo, FullyEvent<PrimaryKey>, string, Task>>(body, parames).Compile();
            });
            //加载Event参数
            static void LdEventArgs(SwitchMethodEmit item, ILGenerator gen)
            {
                if (item.Index > 3)
                {
                    if (item.DeclareLocal.LocalIndex > 0 && item.DeclareLocal.LocalIndex <= 255)
                    {
                        gen.Emit(OpCodes.Ldloc_S, item.DeclareLocal);
                    }
                    else
                    {
                        gen.Emit(OpCodes.Ldloc, item.DeclareLocal);
                    }
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

            static void GetInheritor(SwitchMethodEmit from, List<SwitchMethodEmit> list, List<SwitchMethodEmit> result)
            {
                var inheritorList = list.Where(m => m.CaseType.BaseType == from.CaseType);
                foreach (var inheritor in inheritorList)
                {
                    result.Add(inheritor);
                    GetInheritor(inheritor, list, result);
                }
            }
        }

        /// <summary>
        /// 未处理事件列表
        /// </summary>
        private List<FullyEvent<PrimaryKey>> UnprocessedEventList { get; set; }

        private PrimaryKey GrainId;
        private bool GrainIdAcquired = false;

        public PrimaryKey GrainId
        {
            get
            {
                if (!this.GrainIdAcquired)
                {
                    var type = typeof(PrimaryKey);
                    if (type == typeof(long) && this.GetPrimaryKeyLong() is PrimaryKey longKey)
                    {
                        this.GrainId = longKey;
                    }
                    else if (type == typeof(string) && this.GetPrimaryKeyString() is PrimaryKey stringKey)
                    {
                        this.GrainId = stringKey;
                    }
                    else if (type == typeof(Guid) && this.GetPrimaryKey() is PrimaryKey guidKey)
                    {
                        this.GrainId = guidKey;
                    }
                    else
                    {
                        throw new ArgumentOutOfRangeException(typeof(PrimaryKey).FullName);
                    }

                    this.GrainIdAcquired = true;
                }

                return this.GrainId;
            }
        }

        protected CoreOptions ConfigOptions { get; private set; }

        protected ILogger Logger { get; private set; }

        protected ISerializer Serializer { get; private set; }

        protected ITypeFinder TypeFinder { get; private set; }

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
        /// 指标收集器
        /// </summary>
        protected IMetricMonitor MetricMonitor { get; private set; }

        /// <summary>
        /// 所在的组
        /// </summary>
        protected string Group { get; private set; }

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
        /// <returns></returns>
        protected async virtual ValueTask DependencyInjection()
        {
            this.ConfigOptions = this.ServiceProvider.GetOptionsByName<CoreOptions>(typeof(MainGrain).FullName);
            this.Serializer = this.ServiceProvider.GetService<ISerializer>();
            this.TypeFinder = this.ServiceProvider.GetService<ITypeFinder>();
            this.Logger = (ILogger)this.ServiceProvider.GetService(typeof(ILogger<>).MakeGenericType(this.GrainType));
            this.Group = this.ServiceProvider.GetService<IObserverUnitContainer>().GetUnit<PrimaryKey>(typeof(MainGrain)).GetGroup(this.GrainType);
            this.MetricMonitor = this.ServiceProvider.GetService<IMetricMonitor>();
            var configureBuilder = this.ServiceProvider.GetService<IConfigureBuilder<PrimaryKey, MainGrain>>();
            var storageConfigTask = configureBuilder.GetConfig(this.ServiceProvider, this.GrainId);
            if (!storageConfigTask.IsCompletedSuccessfully)
            {
                await storageConfigTask;
            }

            var storageFactory = this.ServiceProvider.GetService(configureBuilder.StorageFactory) as IStorageFactory;
            //创建事件存储器
            var eventStorageTask = storageFactory.CreateEventStorage(storageConfigTask.Result, this.GrainId);
            if (!eventStorageTask.IsCompletedSuccessfully)
            {
                await eventStorageTask;
            }

            this.EventStorage = eventStorageTask.Result;
            //创建状态存储器
            var followConfigTask = configureBuilder.GetObserverConfig(this.ServiceProvider, this.GrainType, this.GrainId);
            if (!followConfigTask.IsCompletedSuccessfully)
            {
                await followConfigTask;
            }

            var stateStorageTask = storageFactory.CreateObserverSnapshotStorage(followConfigTask.Result, this.GrainId);
            if (!stateStorageTask.IsCompletedSuccessfully)
            {
                await stateStorageTask;
            }

            this.ObserverSnapshotStorage = stateStorageTask.Result;
        }

        public override async Task OnActivateAsync()
        {
            var dITask = this.DependencyInjection();
            if (!dITask.IsCompletedSuccessfully)
            {
                await dITask;
            }

            if (this.ConcurrentHandle)
            {
                this.UnprocessedEventList = new List<FullyEvent<PrimaryKey>>();
            }

            try
            {
                await this.ReadSnapshotAsync();
                if (this.FullyActive)
                {
                    await this.RecoveryFromStorage();
                }

                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("Activation completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "Activation failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                throw;
            }
        }

        /// <summary>
        /// 从库里恢复
        /// </summary>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        private async Task RecoveryFromStorage()
        {
            while (true)
            {
                var eventList = await this.EventStorage.GetList(this.GrainId, this.Snapshot.StartTimestamp, this.Snapshot.Version + 1, this.Snapshot.Version + this.ConfigOptions.NumberOfEventsPerRead);
                await this.UnsafeTell(eventList);
                if (eventList.Count < this.ConfigOptions.NumberOfEventsPerRead)
                {
                    break;
                }
            }

        }

        public override Task OnDeactivateAsync()
        {
            var needSaveSnap = this.Snapshot.Version - this.SnapshotEventVersion >= 1;
            if (needSaveSnap)
            {
                var saveTask = this.SaveSnapshotAsync(true);
                if (!saveTask.IsCompletedSuccessfully)
                {
                    return saveTask.AsTask();
                }
            }

            if (this.Logger.IsEnabled(LogLevel.Trace))
            {
                this.Logger.LogTrace("Deactivate completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
            }

            return Task.CompletedTask;
        }

        protected virtual async Task UnsafeTell(IEnumerable<FullyEvent<PrimaryKey>> eventList)
        {
            if (this.ConcurrentHandle)
            {
                await Task.WhenAll(eventList.Select(@event =>
                {
                    var task = this.EventDelivered(@event);
                    if (!task.IsCompletedSuccessfully)
                    {
                        return task.AsTask();
                    }
                    else
                    {
                        return Task.CompletedTask;
                    }
                }));
                var lastEvt = eventList.Last();
                this.Snapshot.UnsafeUpdateVersion(lastEvt.BasicInfo);
            }
            else
            {
                foreach (var @event in eventList)
                {
                    this.Snapshot.IncrementDoingVersion(this.GrainType);//标记将要处理的Version
                    var task = this.EventDelivered(@event);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

                    this.Snapshot.UpdateVersion(@event.BasicInfo, this.GrainType);//更新处理完成的Version
                }
            }

            var saveTask = this.SaveSnapshotAsync();
            if (!saveTask.IsCompletedSuccessfully)
            {
                await saveTask;
            }
        }

        protected virtual async Task ReadSnapshotAsync()
        {
            try
            {
                this.Snapshot = await this.ObserverSnapshotStorage.Get(this.GrainId);
                if (this.Snapshot == null)
                {
                    var createTask = this.InitFirstSnapshot();
                    if (!createTask.IsCompletedSuccessfully)
                    {
                        await createTask;
                    }
                }

                this.SnapshotEventVersion = this.Snapshot.Version;
                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("ReadSnapshot completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "ReadSnapshot failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                throw;
            }
        }

        /// <summary>
        /// 初始化状态，必须实现
        /// </summary>
        /// <returns></returns>
        protected virtual ValueTask InitFirstSnapshot()
        {
            this.Snapshot = new ObserverSnapshot<PrimaryKey>
            {
                StateId = this.GrainId
            };
            return Consts.ValueTaskDone;
        }

        #endregion
        public Task OnNext(Immutable<byte[]> bytes)
        {
            return this.OnNext(bytes.Value);
        }

        public async Task OnNext(Immutable<List<byte[]>> items)
        {
            if (this.ConcurrentHandle)
            {
                var startVersion = this.Snapshot.Version;
                if (this.UnprocessedEventList.Count > 0)
                {
                    startVersion = this.UnprocessedEventList.Last().BasicInfo.Version;
                }

                var evtList = items.Value.Select(bytes =>
                {
                    if (EventConverter.TryParseWithNoId(bytes, out var transport))
                    {
                        var msgType = this.TypeFinder.FindType(transport.EventUniqueName);
                        var data = this.Serializer.Deserialize(transport.EventBytes, msgType);
                        if (data is IEvent @event)
                        {
                            var eventBase = transport.BaseBytes.ParseToEventBase();
                            if (eventBase.Version > startVersion)
                            {
                                return new FullyEvent<PrimaryKey>
                                {
                                    StateId = this.GrainId,
                                    BasicInfo = eventBase,
                                    Event = @event
                                };
                            }
                        }
                        else
                        {
                            if (this.Logger.IsEnabled(LogLevel.Information))
                            {
                                this.Logger.LogInformation("Non-Event: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(data, msgType));
                            }
                        }
                    }
                    else
                    {
                        if (this.Logger.IsEnabled(LogLevel.Information))
                        {
                            this.Logger.LogInformation($"{nameof(EventConverter.TryParseWithNoId)} failed");
                        }
                    }

                    return default;
                }).Where(o => o != null).OrderBy(o => o.BasicInfo.Version).ToList();
                await this.ConcurrentTell(evtList);
                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("OnNext concurrent completed: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(evtList));
                }
            }
            else
            {
                foreach (var bytes in items.Value)
                {
                    await this.OnNext(bytes);
                }
            }
        }

        private Task OnNext(byte[] bytes)
        {
            if (EventConverter.TryParseWithNoId(bytes, out var transport))
            {
                var msgType = this.TypeFinder.FindType(transport.EventUniqueName);
                var data = this.Serializer.Deserialize(transport.EventBytes, msgType);
                if (data is IEvent @event)
                {
                    var eventBase = transport.BaseBytes.ParseToEventBase();
                    if (eventBase.Version > this.Snapshot.Version)
                    {
                        var tellTask = this.Tell(new FullyEvent<PrimaryKey>
                        {
                            StateId = this.GrainId,
                            BasicInfo = eventBase,
                            Event = @event
                        });
                        return tellTask.AsTask();
                    }

                    if (this.Logger.IsEnabled(LogLevel.Trace))
                    {
                        this.Logger.LogTrace("OnNext completed: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(data, msgType));
                    }
                }
                else
                {
                    if (this.Logger.IsEnabled(LogLevel.Information))
                    {
                        this.Logger.LogInformation("Non-Event: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(data, msgType));
                    }
                }
            }
            else
            {
                if (this.Logger.IsEnabled(LogLevel.Information))
                {
                    this.Logger.LogInformation($"{nameof(EventConverter.TryParseWithNoId)} failed");
                }
            }

            return Task.CompletedTask;
        }

        public Task<long> GetVersion()
        {
            return Task.FromResult(this.Snapshot.Version);
        }

        public async Task<long> GetAndSaveVersion(long compareVersion)
        {
            if (this.SnapshotEventVersion < compareVersion && this.Snapshot.Version >= compareVersion)
            {
                var saveTask = this.SaveSnapshotAsync(true);
                if (!saveTask.IsCompletedSuccessfully)
                {
                    await saveTask;
                }
            }

            return this.Snapshot.Version;
        }

        public async Task<bool> SyncFromObservable(long compareVersion)
        {
            if (this.Snapshot.Version < compareVersion)
            {
                await this.RecoveryFromStorage();
            }

            return this.Snapshot.Version == compareVersion;
        }

        protected async ValueTask Tell(FullyEvent<PrimaryKey> fullyEvent)
        {
            try
            {
                if (fullyEvent.BasicInfo.Version == this.Snapshot.Version + 1)
                {
                    var task = this.EventDelivered(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

                    this.Snapshot.FullUpdateVersion(fullyEvent.BasicInfo, this.GrainType);//更新处理完成的Version
                }
                else if (fullyEvent.BasicInfo.Version > this.Snapshot.Version)
                {
                    var eventList = await this.EventStorage.GetList(this.GrainId, this.Snapshot.StartTimestamp, this.Snapshot.Version + 1, fullyEvent.BasicInfo.Version - 1);
                    foreach (var evt in eventList)
                    {
                        var task = this.EventDelivered(evt);
                        if (!task.IsCompletedSuccessfully)
                        {
                            await task;
                        }

                        this.Snapshot.FullUpdateVersion(evt.BasicInfo, this.GrainType);//更新处理完成的Version
                    }
                }

                if (fullyEvent.BasicInfo.Version == this.Snapshot.Version + 1)
                {
                    var task = this.EventDelivered(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

                    this.Snapshot.FullUpdateVersion(fullyEvent.BasicInfo, this.GrainType);//更新处理完成的Version
                }

                if (fullyEvent.BasicInfo.Version > this.Snapshot.Version)
                {
                    throw new EventVersionUnorderedException(this.GrainId.ToString(), this.GrainType, fullyEvent.BasicInfo.Version, this.Snapshot.Version);
                }

                var saveTask = this.SaveSnapshotAsync();
                if (!saveTask.IsCompletedSuccessfully)
                {
                    await saveTask;
                }
            }
            catch
            {
                var saveTask = this.SaveSnapshotAsync(true);
                if (!saveTask.IsCompletedSuccessfully)
                {
                    await saveTask;
                }

                throw;
            }
        }

        private async Task ConcurrentTell(List<FullyEvent<PrimaryKey>> evtList)
        {
            var startVersion = this.Snapshot.Version;
            if (this.UnprocessedEventList.Count > 0)
            {
                startVersion = this.UnprocessedEventList.Last().BasicInfo.Version;
            }

            if (evtList.Count > 0)
            {
                var inputLast = evtList.Last();
                if (startVersion + evtList.Count < inputLast.BasicInfo.Version)
                {
                    var loadList = await this.EventStorage.GetList(this.GrainId, 0, startVersion + 1, inputLast.BasicInfo.Version - 1);
                    this.UnprocessedEventList.AddRange(loadList);
                    this.UnprocessedEventList.Add(inputLast);
                }
                else
                {
                    this.UnprocessedEventList.AddRange(evtList);
                }
            }

            if (this.UnprocessedEventList.Count > 0)
            {
                await Task.WhenAll(this.UnprocessedEventList.Select(@event =>
                {
                    var task = this.EventDelivered(@event);
                    if (!task.IsCompletedSuccessfully)
                    {
                        return task.AsTask();
                    }

                    return Task.CompletedTask;
                }));
                this.Snapshot.UnsafeUpdateVersion(this.UnprocessedEventList.Last().BasicInfo);
                var saveTask = this.SaveSnapshotAsync();
                if (!saveTask.IsCompletedSuccessfully)
                {
                    await saveTask;
                }

                this.UnprocessedEventList.Clear();
            }
        }

        protected virtual async ValueTask EventDelivered(FullyEvent<PrimaryKey> fullyEvent)
        {
            try
            {
                if (this.SnapshotEventVersion > 0 &&
                    this.Snapshot.Version > 0 &&
                    fullyEvent.BasicInfo.Timestamp < this.Snapshot.StartTimestamp)
                {
                    await this.ObserverSnapshotStorage.UpdateStartTimestamp(this.Snapshot.StateId, fullyEvent.BasicInfo.Timestamp);
                }

                if (this.MetricMonitor != default)
                {
                    var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    var task = this.OnEventDelivered(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

                    this.MetricMonitor.Report(new FollowMetricElement
                    {
                        Actor = this.GrainType.Name,
                        Event = fullyEvent.Event.GetType().Name,
                        FromActor = typeof(MainGrain).Name,
                        ElapsedMs = (int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - startTime),
                        DeliveryElapsedMs = (int)(startTime - fullyEvent.BasicInfo.Timestamp),
                        Group = this.Group
                    });
                }
                else
                {
                    var task = this.OnEventDelivered(fullyEvent);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }
                }
            }
            catch (Exception ex)
            {
                this.Logger.LogCritical(ex, "Delivered failed: {0}->{1}->{2}", this.GrainType.FullName, this.GrainId.ToString(), this.Serializer.Serialize(fullyEvent, fullyEvent.GetType()));
                throw ex;
            }
        }

        public Task DefaultHandler(IEvent evt)
        {
            if (this.handlerAttribute is null || !this.handlerAttribute.Ignores.Contains(evt.GetType()))
            {
                throw new UnfindEventHandlerException(evt.GetType());
            }

            return Task.CompletedTask;
        }

        protected virtual ValueTask OnEventDelivered(FullyEvent<PrimaryKey> fullyEvent)
        {
            return new ValueTask(this.handlerInvokeFunc(this, fullyEvent.Event, fullyEvent.BasicInfo, fullyEvent, typeof(MainGrain).Name));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSaveSnapshot() => Consts.ValueTaskDone;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual ValueTask OnSavedSnapshot() => new ValueTask();

        protected virtual async ValueTask SaveSnapshotAsync(bool force = false)
        {
            if (this.SaveSnapshot)
            {
                if ((force && this.Snapshot.Version > this.SnapshotEventVersion) ||
                    (this.Snapshot.Version - this.SnapshotEventVersion >= this.ConfigOptions.ObserverSnapshotVersionInterval))
                {
                    try
                    {
                        var onSaveSnapshotTask = this.OnSaveSnapshot();//自定义保存项
                        if (!onSaveSnapshotTask.IsCompletedSuccessfully)
                        {
                            await onSaveSnapshotTask;
                        }

                        if (this.MetricMonitor != default)
                        {
                            var startTime = DateTimeOffset.UtcNow;
                            if (this.SnapshotEventVersion == 0)
                            {
                                await this.ObserverSnapshotStorage.Insert(this.Snapshot);
                            }
                            else
                            {
                                await this.ObserverSnapshotStorage.Update(this.Snapshot);
                            }

                            var metric = new SnapshotMetricElement
                            {
                                Actor = this.GrainType.Name,
                                ElapsedVersion = (int)(this.Snapshot.Version - this.SnapshotEventVersion),
                                SaveElapsedMs = (int)DateTimeOffset.UtcNow.Subtract(startTime).TotalMilliseconds,
                                Snapshot = typeof(ObserverSnapshot<PrimaryKey>).Name
                            };
                            this.MetricMonitor.Report(metric);
                        }
                        else
                        {
                            if (this.SnapshotEventVersion == 0)
                            {
                                await this.ObserverSnapshotStorage.Insert(this.Snapshot);
                            }
                            else
                            {
                                await this.ObserverSnapshotStorage.Update(this.Snapshot);
                            }
                        }

                        this.SnapshotEventVersion = this.Snapshot.Version;
                        var onSavedSnapshotTask = this.OnSavedSnapshot();
                        if (!onSavedSnapshotTask.IsCompletedSuccessfully)
                        {
                            await onSavedSnapshotTask;
                        }

                        if (this.Logger.IsEnabled(LogLevel.Trace))
                        {
                            this.Logger.LogTrace("SaveSnapshot completed: {0}->{1}", this.GrainType.FullName, this.Serializer.Serialize(this.Snapshot));
                        }
                    }
                    catch (Exception ex)
                    {
                        this.Logger.LogCritical(ex, "SaveSnapshot failed: {0}->{1}", this.GrainType.FullName, this.GrainId.ToString());
                        throw;
                    }
                }
            }
        }

        public virtual async Task Reset()
        {
            await this.ObserverSnapshotStorage.Delete(this.GrainId);
            if (this.ConcurrentHandle)
            {
                this.UnprocessedEventList.Clear();
            }

            await this.ReadSnapshotAsync();
        }
    }
}
