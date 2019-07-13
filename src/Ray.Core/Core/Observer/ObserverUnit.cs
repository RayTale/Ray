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
        readonly IClusterClient clusterClient;
        readonly Dictionary<string, List<Func<byte[], Task>>> eventHandlerGroups = new Dictionary<string, List<Func<byte[], Task>>>();
        readonly List<Func<byte[], Task>> eventHandlers = new List<Func<byte[], Task>>();
        readonly List<Func<PrimaryKey, long, Task<long>>> observerVersionHandlers = new List<Func<PrimaryKey, long, Task<long>>>();
        public Type GrainType { get; }

        public ObserverUnit(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
            clusterClient = serviceProvider.GetService<IClusterClient>();
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
            Task func(byte[]  bytes)
            {
                var (success, transport) = EventBytesTransport.FromBytes<PrimaryKey>(bytes);
                if (success)
                {
                    var data = serializer.Deserialize(TypeContainer.GetType(transport.EventTypeCode), transport.EventBytes);
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
            observerVersionHandlers.Add((actorId, version) => GetVersion(observerType, actorId).GetAndSaveVersion(version));
            //内部函数
            Task func(byte[] bytes)
            {
                var (success, actorId) = EventBytesTransport.GetActorId<PrimaryKey>(bytes);
                if (success)
                {
                    if (typeof(IObserver).IsAssignableFrom(observerType))
                    {
                        return GetObserver(observerType, actorId).OnNext(new Immutable<byte[]>(bytes));
                    }
                    else if (typeof(IConcurrentObserver).IsAssignableFrom(observerType))
                    {
                        return GetConcurrentObserver(observerType, actorId).OnNext(new Immutable<byte[]>(bytes));
                    }
                    else
                        throw new NotSupportedException($"{observerType.FullName} must inheritance from 'IConcurrentObserver' or 'IObserver'");
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
        private IVersion GetVersion(Type observerType, PrimaryKey primaryKey)
        {
            if (typeof(IObserver).IsAssignableFrom(observerType))
            {
                return GetObserver(observerType, primaryKey);
            }
            else if (typeof(IConcurrentObserver).IsAssignableFrom(observerType))
            {
                return GetConcurrentObserver(observerType, primaryKey);
            }
            else
                throw new NotSupportedException($"{observerType.FullName} must inheritance from 'IConcurrentObserver' or 'IObserver'");
        }
        static readonly ConcurrentDictionary<Type, Func<IClusterClient, PrimaryKey, string, IObserver>> _observerGeneratorDict = new ConcurrentDictionary<Type, Func<IClusterClient, PrimaryKey, string, IObserver>>();
        private IObserver GetObserver(Type ObserverType, PrimaryKey primaryKey)
        {
            var func = _observerGeneratorDict.GetOrAdd(ObserverType, key =>
            {
                var clientType = typeof(IClusterClient);
                var clientParams = Expression.Parameter(clientType, "client");
                var primaryKeyParams = Expression.Parameter(typeof(PrimaryKey), "primaryKey");
                var grainClassNamePrefixParams = Expression.Parameter(typeof(string), "grainClassNamePrefix");
                var method = typeof(ClusterClientExtensions).GetMethod("GetGrain", new Type[] { clientType, typeof(PrimaryKey), typeof(string) });
                var body = Expression.Call(method.MakeGenericMethod(ObserverType), clientParams, primaryKeyParams, grainClassNamePrefixParams);
                return Expression.Lambda<Func<IClusterClient, PrimaryKey, string, IObserver>>(body, clientParams, primaryKeyParams, grainClassNamePrefixParams).Compile();
            });
            return func(clusterClient, primaryKey, null);
        }
        static readonly ConcurrentDictionary<Type, Func<IClusterClient, PrimaryKey, string, IConcurrentObserver>> _ConcurrentObserverGeneratorDict = new ConcurrentDictionary<Type, Func<IClusterClient, PrimaryKey, string, IConcurrentObserver>>();
        private IConcurrentObserver GetConcurrentObserver(Type ObserverType, PrimaryKey primaryKey)
        {
            var func = _ConcurrentObserverGeneratorDict.GetOrAdd(ObserverType, key =>
            {
                var clientType = typeof(IClusterClient);
                var clientParams = Expression.Parameter(clientType, "client");
                var primaryKeyParams = Expression.Parameter(typeof(PrimaryKey), "primaryKey");
                var grainClassNamePrefixParams = Expression.Parameter(typeof(string), "grainClassNamePrefix");
                var method = typeof(ClusterClientExtensions).GetMethod("GetGrain", new Type[] { clientType, typeof(PrimaryKey), typeof(string) });
                var body = Expression.Call(method.MakeGenericMethod(ObserverType), clientParams, primaryKeyParams, grainClassNamePrefixParams);
                return Expression.Lambda<Func<IClusterClient, PrimaryKey, string, IConcurrentObserver>>(body, clientParams, primaryKeyParams, grainClassNamePrefixParams).Compile();
            });
            return func(clusterClient, primaryKey, null);
        }
    }
    public static class ClusterClientExtensions
    {
        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, Guid primaryKey, string grainClassNamePrefix = null) where TGrainInterface : IGrainWithGuidKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix);
        }
        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, long primaryKey, string grainClassNamePrefix = null) where TGrainInterface : IGrainWithIntegerKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix);
        }
        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, string primaryKey, string grainClassNamePrefix = null) where TGrainInterface : IGrainWithStringKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix);
        }
        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, Guid primaryKey, string keyExtension, string grainClassNamePrefix = null) where TGrainInterface : IGrainWithGuidCompoundKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix);
        }
        public static TGrainInterface GetGrain<TGrainInterface>(IClusterClient client, long primaryKey, string keyExtension, string grainClassNamePrefix = null) where TGrainInterface : IGrainWithIntegerCompoundKey
        {
            return client.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix);
        }
    }
}
