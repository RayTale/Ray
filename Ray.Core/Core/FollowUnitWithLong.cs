using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;

namespace Ray.Core
{
    public class FollowUnitWithLong : IFollowUnit<long>
    {
        readonly IServiceProvider serviceProvider;
        readonly Dictionary<string, List<Func<byte[], Task>>> eventHandlersDict = new Dictionary<string, List<Func<byte[], Task>>>();
        readonly List<Func<byte[], Task>> eventHandlers = new List<Func<byte[], Task>>();
        readonly List<Func<long, long, Task<long>>> followVersionHandlers = new List<Func<long, long, Task<long>>>();

        public Type GrainType { get; }

        public FollowUnitWithLong(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
            GrainType = grainType;
        }
        public static FollowUnitWithLong From<Grain>(IServiceProvider serviceProvider)
            where Grain : Orleans.Grain
        {
            return new FollowUnitWithLong(serviceProvider, typeof(Grain));
        }
        public List<Func<byte[], Task>> GetEventHandlers(string followType)
        {
            if (!eventHandlersDict.TryGetValue(followType, out var funcs))
            {
                funcs = new List<Func<byte[], Task>>();
                eventHandlersDict.Add(followType, funcs);
            }
            return funcs;
        }

        public List<Func<long, long, Task<long>>> GetAndSaveVersionFuncs()
        {
            return followVersionHandlers;
        }
        public FollowUnitWithLong BindEventHandler(string followType, Func<byte[], Task> handler)
        {
            var funcs = GetEventHandlers(followType);
            funcs.Add(handler);
            eventHandlers.Add(handler);
            return this;
        }
        public FollowUnitWithLong Flow<F>(string followType)
            where F : IFollow, IGrainWithIntegerKey
        {
            var funcs = GetEventHandlers(followType);
            Task func(byte[] bytes)
            {
                var (success, actorId) = EventBytesTransport.GetActorIdWithLong(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).Tell(bytes);
                }
                return Task.CompletedTask;
            }
            funcs.Add(func);
            eventHandlers.Add(func);
            followVersionHandlers.Add((stateId, version) => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).GetAndSaveVersion(version));
            return this;
        }
        public FollowUnitWithLong ConcurrentFlow<F>(string followType)
            where F : IConcurrentFollow, IGrainWithIntegerKey
        {
            var funcs = GetEventHandlers(followType);
            Task func(byte[] bytes)
            {
                var (success, actorId) = EventBytesTransport.GetActorIdWithLong(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).ConcurrentTell(bytes);
                }
                return Task.CompletedTask;
            }
            funcs.Add(func);
            eventHandlers.Add(func);
            followVersionHandlers.Add((stateId, version) => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).GetAndSaveVersion(version));
            return this;
        }
        public List<Func<byte[], Task>> GetAllEventHandlers()
        {
            return eventHandlers;
        }
    }
}
