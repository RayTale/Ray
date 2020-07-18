using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Abstractions;
using Ray.Core.Event;
using Ray.Core.EventBus;
using Ray.Core.Observer;
using Ray.Core.Serialization;

namespace Ray.Core
{
    public class ObserverUnit<PrimaryKey> : IObserverUnit<PrimaryKey>
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ISerializer serializer;
        private readonly ITypeFinder typeFinder;
        private readonly IClusterClient clusterClient;
        private readonly Dictionary<string, List<Func<BytesBox, Task>>> eventHandlerGroups = new Dictionary<string, List<Func<BytesBox, Task>>>();
        private readonly Dictionary<string, List<Func<List<BytesBox>, Task>>> batchEventHandlerGroups = new Dictionary<string, List<Func<List<BytesBox>, Task>>>();
        private readonly List<Func<BytesBox, Task>> eventHandlers = new List<Func<BytesBox, Task>>();
        private readonly List<Func<List<BytesBox>, Task>> batchEventHandlers = new List<Func<List<BytesBox>, Task>>();
        private readonly List<Func<PrimaryKey, long, Task<long>>> observerVersionHandlers = new List<Func<PrimaryKey, long, Task<long>>>();
        private readonly List<Func<PrimaryKey, Task>> observerResetHandlers = new List<Func<PrimaryKey, Task>>();
        private readonly List<Func<PrimaryKey, long, Task<bool>>> observerSyncHandlers = new List<Func<PrimaryKey, long, Task<bool>>>();
        private readonly Dictionary<Type, string> observerGroupDict = new Dictionary<Type, string>();

        protected ILogger Logger { get; private set; }

        public Type GrainType { get; }

        public ObserverUnit(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
            this.clusterClient = serviceProvider.GetService<IClusterClient>();
            this.serializer = serviceProvider.GetService<ISerializer>();
            this.typeFinder = serviceProvider.GetService<ITypeFinder>();
            this.Logger = serviceProvider.GetService<ILogger<ObserverUnit<PrimaryKey>>>();
            this.GrainType = grainType;
        }

        public static ObserverUnit<PrimaryKey> From<Grain>(IServiceProvider serviceProvider)
            where Grain : Orleans.Grain
        {
            return new ObserverUnit<PrimaryKey>(serviceProvider, typeof(Grain));
        }

        public Task<long[]> GetAndSaveVersion(PrimaryKey primaryKey, long srcVersion)
        {
            return Task.WhenAll(this.observerVersionHandlers.Select(func => func(primaryKey, srcVersion)));
        }

        public Task<bool[]> SyncAllObservers(PrimaryKey primaryKey, long srcVersion)
        {
            return Task.WhenAll(this.observerSyncHandlers.Select(func => func(primaryKey, srcVersion)));
        }

        public Task Reset(PrimaryKey primaryKey)
        {
            return Task.WhenAll(this.observerResetHandlers.Select(func => func(primaryKey)));
        }

        public List<string> GetGroups() => this.eventHandlerGroups.Keys.ToList();

        public List<Func<BytesBox, Task>> GetAllEventHandlers()
        {
            return this.eventHandlers;
        }

        public List<Func<BytesBox, Task>> GetEventHandlers(string observerGroup)
        {
            if (!this.eventHandlerGroups.TryGetValue(observerGroup, out var funcs))
            {
                funcs = new List<Func<BytesBox, Task>>();
                this.eventHandlerGroups.Add(observerGroup, funcs);
            }

            return funcs;
        }

        public List<Func<List<BytesBox>, Task>> GetAllBatchEventHandlers()
        {
            return this.batchEventHandlers;
        }

        public List<Func<List<BytesBox>, Task>> GetBatchEventHandlers(string observerGroup)
        {
            if (!this.batchEventHandlerGroups.TryGetValue(observerGroup, out var funcs))
            {
                funcs = new List<Func<List<BytesBox>, Task>>();
                this.batchEventHandlerGroups.Add(observerGroup, funcs);
            }

            return funcs;

        }

        public ObserverUnit<PrimaryKey> UnreliableObserver(
            string group,
            Func<IServiceProvider,
            FullyEvent<PrimaryKey>, ValueTask> handler)
        {
            this.GetEventHandlers(group).Add(EventHandler);
            this.GetBatchEventHandlers(group).Add(BatchEventHandler);
            this.eventHandlers.Add(EventHandler);
            this.batchEventHandlers.Add(BatchEventHandler);
            return this;
            //内部函数
            Task EventHandler(BytesBox bytes)
            {
                if (EventConverter.TryParse<PrimaryKey>(bytes.Value, out var transport))
                {
                    var data = this.serializer.Deserialize(transport.EventBytes, this.typeFinder.FindType(transport.EventUniqueName));
                    if (data is IEvent @event && transport.GrainId is PrimaryKey actorId)
                    {
                        var eventBase = transport.BaseBytes.ParseToEventBase();
                        var tellTask = handler(this.serviceProvider, new FullyEvent<PrimaryKey>
                        {
                            StateId = actorId,
                            BasicInfo = eventBase,
                            Event = @event
                        });
                        if (!tellTask.IsCompletedSuccessfully)
                        {
                            return tellTask.AsTask();
                        }
                    }

                    bytes.Success = true;
                }

                return Task.CompletedTask;
            }

            Task BatchEventHandler(List<BytesBox> list)
            {
                var groups =
                    list.Select(bytes =>
                    {
                        if (EventConverter.TryParse<PrimaryKey>(bytes.Value, out var transport))
                        {
                            var data = this.serializer.Deserialize(transport.EventBytes, this.typeFinder.FindType(transport.EventUniqueName));
                            if (data is IEvent @event && transport.GrainId is PrimaryKey actorId)
                            {
                                var eventBase = transport.BaseBytes.ParseToEventBase();
                                var fullEvent = new FullyEvent<PrimaryKey>
                                {
                                    StateId = actorId,
                                    BasicInfo = eventBase,
                                    Event = @event
                                };
                                return (bytes, fullEvent);
                            }
                        }

                        return default;
                    })
                    .Where(o => o != default)
                    .GroupBy(o => o.fullEvent.StateId);
                return Task.WhenAll(groups.Select(async groupItems =>
                {
                    foreach (var item in groupItems)
                    {
                        var tellTask = handler(this.serviceProvider, item.fullEvent);
                        if (!tellTask.IsCompletedSuccessfully)
                        {
                            await tellTask;
                        }

                        item.bytes.Success = true;
                    }
                }));
            }
        }

        public void Observer(string group, Type observerType)
        {
            if (!typeof(IObserver).IsAssignableFrom(observerType))
            {
                throw new NotSupportedException($"{observerType.FullName} must inheritance from IObserver");
            }

            this.observerGroupDict.Add(observerType, group);
            this.GetEventHandlers(group).Add(EventHandler);
            this.GetBatchEventHandlers(group).Add(BatchEventHandler);
            this.eventHandlers.Add(EventHandler);
            this.batchEventHandlers.Add(BatchEventHandler);
            this.observerVersionHandlers.Add((actorId, version) => this.GetObserver(observerType, actorId).GetAndSaveVersion(version));
            this.observerSyncHandlers.Add((actorId, version) => this.GetObserver(observerType, actorId).SyncFromObservable(version));
            this.observerResetHandlers.Add((actorId) => this.GetObserver(observerType, actorId).Reset());
            //内部函数
            async Task EventHandler(BytesBox bytes)
            {
                if (EventConverter.TryParseActorId<PrimaryKey>(bytes.Value, out var actorId))
                {
                    await this.GetObserver(observerType, actorId).OnNext(new Immutable<byte[]>(bytes.Value));
                    bytes.Success = true;
                }
                else
                {
                    if (this.Logger.IsEnabled(LogLevel.Error))
                    {
                        this.Logger.LogError($"{nameof(EventConverter.TryParseActorId)} failed");
                    }
                }
            }

            Task BatchEventHandler(List<BytesBox> list)
            {
                var groups = list.Select(bytes =>
                {
                    var success = EventConverter.TryParseActorId<PrimaryKey>(bytes.Value, out var GrainId);
                    if (!success)
                    {
                        if (this.Logger.IsEnabled(LogLevel.Error))
                        {
                            this.Logger.LogError($"{nameof(EventConverter.TryParseActorId)} failed");
                        }
                    }

                    return (success, GrainId, bytes);
                }).Where(o => o.success).GroupBy(o => o.GrainId);
                return Task.WhenAll(groups.Select(async kv =>
                {
                    var items = kv.Select(item => item.bytes.Value).ToList();
                    await this.GetObserver(observerType, kv.Key).OnNext(new Immutable<List<byte[]>>(items));
                    foreach (var item in kv)
                    {
                        item.bytes.Success = true;
                    }
                }));
            }
        }

        public ObserverUnit<PrimaryKey> Observer(string group, params Type[] observers)
        {
            foreach (var observerType in observers)
            {
                this.Observer(group, observerType);
            }

            return this;
        }

        public ObserverUnit<PrimaryKey> Observer<Observer>(string group)
            where Observer : IObserver
        {
            this.Observer(group, typeof(Observer));
            return this;
        }

        private static readonly ConcurrentDictionary<Type, Func<IClusterClient, PrimaryKey, string, IObserver>> observerGeneratorDict = new ConcurrentDictionary<Type, Func<IClusterClient, PrimaryKey, string, IObserver>>();

        private IObserver GetObserver(Type ObserverType, PrimaryKey primaryKey)
        {
            var func = observerGeneratorDict.GetOrAdd(ObserverType, key =>
            {
                var clientType = typeof(IClusterClient);
                var clientParams = Expression.Parameter(clientType, "client");
                var primaryKeyParams = Expression.Parameter(typeof(PrimaryKey), "primaryKey");
                var grainClassNamePrefixParams = Expression.Parameter(typeof(string), "grainClassNamePrefix");
                var method = typeof(ClusterClientExtensions).GetMethod("GetGrain", new Type[] { clientType, typeof(PrimaryKey), typeof(string) });
                var body = Expression.Call(method.MakeGenericMethod(ObserverType), clientParams, primaryKeyParams, grainClassNamePrefixParams);
                return Expression.Lambda<Func<IClusterClient, PrimaryKey, string, IObserver>>(body, clientParams, primaryKeyParams, grainClassNamePrefixParams).Compile();
            });
            return func(this.clusterClient, primaryKey, null);
        }

        public string GetGroup(Type observerType)
        {
            return this.observerGroupDict.Single(kv => kv.Key.IsAssignableFrom(observerType)).Value;
        }
    }

    public static class ClusterClientExtensions
    {
        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, Guid primaryKey, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix);
        }

        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, long primaryKey, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix);
        }

        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, string primaryKey, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithStringKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix);
        }

        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, Guid primaryKey, string keyExtension, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidCompoundKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, keyExtension, grainClassNamePrefix);
        }

        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, long primaryKey, string keyExtension, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerCompoundKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, keyExtension, grainClassNamePrefix);
        }
    }
}
