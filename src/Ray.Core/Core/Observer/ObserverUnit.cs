using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Ray.Core
{
    public class ObserverUnit<PrimaryKey> : IObserverUnit<PrimaryKey>
    {
        readonly IServiceProvider serviceProvider;
        readonly Dictionary<string, List<Func<byte[], Task>>> eventHandlerGroups = new Dictionary<string, List<Func<byte[], Task>>>();
        readonly List<Func<byte[], Task>> eventHandlers = new List<Func<byte[], Task>>();
        readonly List<Func<PrimaryKey, long, Task<long>>> observerVersionHandlers = new List<Func<PrimaryKey, long, Task<long>>>();
        public Type GrainType { get; }

        public ObserverUnit(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
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
        public List<Func<byte[], Task>> GetEventHandlers(string group)
        {
            if (!eventHandlerGroups.TryGetValue(group, out var funcs))
            {
                funcs = new List<Func<byte[], Task>>();
                eventHandlerGroups.Add(group, funcs);
            }
            return funcs;
        }
        public ObserverUnit<PrimaryKey> Observer(string followType, Func<IClusterClient, PrimaryKey, IObserver> grainFunc)
        {
            var funcs = GetEventHandlers(followType);
            funcs.Add(func);
            eventHandlers.Add(func);
            observerVersionHandlers.Add((actorId, version) => grainFunc(serviceProvider.GetService<IClusterClient>(), actorId).GetAndSaveVersion(version));
            return this;
            //内部函数
            Task func(byte[] bytes)
            {
                var (success, actorId) = EventBytesTransport.GetActorId<PrimaryKey>(bytes);
                if (success)
                {
                    return grainFunc(serviceProvider.GetService<IClusterClient>(), actorId).OnNext(bytes);
                }
                return Task.CompletedTask;
            }
        }
        public ObserverUnit<PrimaryKey> ConcurrentObserver(string group, Func<IClusterClient, PrimaryKey, IConcurrentObserver> grainFunc)
        {
            var funcs = GetEventHandlers(group);
            funcs.Add(func);
            eventHandlers.Add(func);
            observerVersionHandlers.Add((actorId, version) => grainFunc(serviceProvider.GetService<IClusterClient>(), actorId).GetAndSaveVersion(version));
            return this;
            //内部函数
            Task func(byte[] bytes)
            {
                var (success, actorId) = EventBytesTransport.GetActorId<PrimaryKey>(bytes);
                if (success)
                {
                    return grainFunc(serviceProvider.GetService<IClusterClient>(), actorId).ConcurrentOnNext(bytes);
                }
                return Task.CompletedTask;
            }
        }
    }
}
