using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core
{
    public class FollowUnit<PrimaryKey> : IFollowUnit<PrimaryKey>
    {
        readonly IServiceProvider serviceProvider;
        readonly Dictionary<string, List<Func<byte[], Task>>> eventHandlerGroups = new Dictionary<string, List<Func<byte[], Task>>>();
        readonly List<Func<byte[], Task>> eventHandlers = new List<Func<byte[], Task>>();
        readonly List<Func<PrimaryKey, long, Task<long>>> followVersionHandlers = new List<Func<PrimaryKey, long, Task<long>>>();
        public Type GrainType { get; }

        public FollowUnit(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
            GrainType = grainType;
        }
        public static FollowUnit<PrimaryKey> From<Grain>(IServiceProvider serviceProvider) where Grain : Orleans.Grain
        {
            return new FollowUnit<PrimaryKey>(serviceProvider, typeof(Grain));
        }
        public List<Func<byte[], Task>> GetAllEventHandlers()
        {
            return eventHandlers;
        }

        public List<Func<PrimaryKey, long, Task<long>>> GetAndSaveVersionFuncs()
        {
            return followVersionHandlers;
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
        public FollowUnit<PrimaryKey> Flow(string followType, Func<IClusterClient, PrimaryKey, IFollow> grainFunc)
        {
            var funcs = GetEventHandlers(followType);
            funcs.Add(func);
            eventHandlers.Add(func);
            followVersionHandlers.Add((actorId, version) => grainFunc(serviceProvider.GetService<IClusterClient>(), actorId).GetAndSaveVersion(version));
            return this;
            //内部函数
            Task func(byte[] bytes)
            {
                var (success, actorId) = EventBytesTransport.GetActorId<PrimaryKey>(bytes);
                if (success)
                {
                    return grainFunc(serviceProvider.GetService<IClusterClient>(), actorId).Tell(bytes);
                }
                return Task.CompletedTask;
            }
        }
        public FollowUnit<PrimaryKey> ConcurrentFlow(string group, Func<IClusterClient, PrimaryKey, IConcurrentFollow> grainFunc)
        {
            var funcs = GetEventHandlers(group);
            funcs.Add(func);
            eventHandlers.Add(func);
            followVersionHandlers.Add((actorId, version) => grainFunc(serviceProvider.GetService<IClusterClient>(), actorId).GetAndSaveVersion(version));
            return this;
            //内部函数
            Task func(byte[] bytes)
            {
                var (success, actorId) = EventBytesTransport.GetActorId<PrimaryKey>(bytes);
                if (success)
                {
                    return grainFunc(serviceProvider.GetService<IClusterClient>(), actorId).ConcurrentTell(bytes);
                }
                return Task.CompletedTask;
            }
        }
    }
}
