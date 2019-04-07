using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Event;
using Ray.Core.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Ray.Core
{
    public class ObserverUnit<PrimaryKey> : IObserverUnit<PrimaryKey>
    {
        readonly IServiceProvider serviceProvider;
        readonly ISerializer serializer;
        readonly Dictionary<string, List<Func<byte[], Task>>> eventHandlerGroups = new Dictionary<string, List<Func<byte[], Task>>>();
        readonly List<Func<byte[], Task>> eventHandlers = new List<Func<byte[], Task>>();
        readonly List<Func<PrimaryKey, long, Task<long>>> observerVersionHandlers = new List<Func<PrimaryKey, long, Task<long>>>();
        public Type GrainType { get; }

        public ObserverUnit(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
            serializer = serviceProvider.GetService<ISerializer>();
            GrainType = grainType;
        }
        public static ObserverUnit<PrimaryKey> From<Grain>(IServiceProvider serviceProvider) where Grain : Orleans.Grain
        {
            return new ObserverUnit<PrimaryKey>(serviceProvider, typeof(Grain));
        }
        public List<Func<byte[], Task>> GetAllEventHandlers()
        {
            return eventHandlers;
        }
        public Task<long[]> GetAndSaveVersion(PrimaryKey primaryKey, long srcVersion)
        {
            return Task.WhenAll(observerVersionHandlers.Select(func => func(primaryKey, srcVersion)));
        }
        public List<string> GetGroups() => eventHandlerGroups.Keys.ToList();
        public List<Func<byte[], Task>> GetEventHandlers(string group)
        {
            if (!eventHandlerGroups.TryGetValue(group, out var funcs))
            {
                funcs = new List<Func<byte[], Task>>();
                eventHandlerGroups.Add(group, funcs);
            }
            return funcs;
        }
        public ObserverUnit<PrimaryKey> UnreliableObserver(string group, Func<IServiceProvider, IFullyEvent<PrimaryKey>, ValueTask> handler)
        {
            var funcs = GetEventHandlers(group);
            funcs.Add(func);
            eventHandlers.Add(func);
            return this;
            //内部函数
            Task func(byte[] bytes)
            {
                var (success, transport) = EventBytesTransport.FromBytes<PrimaryKey>(bytes);
                if (success)
                {
                    var data = serializer.Deserialize(TypeContainer.GetType(transport.EventType), transport.EventBytes);
                    if (data is IEvent @event && transport.GrainId is PrimaryKey actorId)
                    {
                        var eventBase = EventBase.FromBytes(transport.BaseBytes);
                        var tellTask = handler(serviceProvider, new FullyEvent<PrimaryKey>
                        {
                            StateId = actorId,
                            Base = eventBase,
                            Event = @event
                        });
                        if (!tellTask.IsCompletedSuccessfully)
                            return tellTask.AsTask();
                    }
                }
                return Task.CompletedTask;
            }
        }
        public void Observer(string group, Type observerType)
        {
            var funcs = GetEventHandlers(group);
            funcs.Add(func);
            eventHandlers.Add(func);
            observerVersionHandlers.Add((actorId, version) => GetObserver(observerType, actorId).GetAndSaveVersion(version));
            //内部函数
            Task func(byte[] bytes)
            {
                var (success, actorId) = EventBytesTransport.GetActorId<PrimaryKey>(bytes);
                if (success)
                {
                    var observer = GetObserver(observerType, actorId);
                    if (observer is IConcurrentObserver concurrentObserver)
                        return concurrentObserver.ConcurrentOnNext(new Immutable<byte[]>(bytes));
                    return observer.OnNext(new Immutable<byte[]>(bytes));
                }
                return Task.CompletedTask;
            }
        }
        public ObserverUnit<PrimaryKey> Observer(string group, params Type[] observers)
        {
            foreach (var observerType in observers)
            {
                Observer(group, observerType);
            }
            return this;
        }
        public ObserverUnit<PrimaryKey> Observer<Observer>(string group)
            where Observer : IObserver
        {
            this.Observer(group, typeof(Observer));
            return this;
        }
        static readonly ConcurrentDictionary<Type, Func<IClusterClient, PrimaryKey, IObserver>> _FuncDict = new ConcurrentDictionary<Type, Func<IClusterClient, PrimaryKey, IObserver>>();
        private IObserver GetObserver(Type ObserverType, PrimaryKey primaryKey)
        {
            var clusterClient = serviceProvider.GetService<IClusterClient>();
            var getGrainFunc = _FuncDict.GetOrAdd(ObserverType, key =>
            {
                var clientType = clusterClient.GetType();
                var primaryKeyParams = Expression.Parameter(typeof(PrimaryKey), "primaryKey");
                var grainClassNamePrefixParams = Expression.Parameter(typeof(string), "grainClassNamePrefix");
                var instanceParams = Expression.Parameter(clientType);
                var method = clientType.GetMethod("GetGrain", new Type[] { typeof(PrimaryKey), typeof(string) });
                var body = Expression.Call(instanceParams, method.MakeGenericMethod(ObserverType), primaryKeyParams, grainClassNamePrefixParams);
                var func = Expression.Lambda(body, instanceParams, primaryKeyParams, grainClassNamePrefixParams).Compile();
                return (client, id) => func.DynamicInvoke(client, id, null) as IObserver;
            });
            return getGrainFunc(clusterClient, primaryKey);
        }
    }
}
