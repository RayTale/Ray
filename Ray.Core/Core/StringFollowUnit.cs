using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;

namespace Ray.Core
{
    public class StringFollowUnit<E> : IFollowUnit<string>
    {
        readonly IServiceProvider serviceProvider;
        readonly Dictionary<string, List<Func<byte[], Task>>> eventHandlers = new Dictionary<string, List<Func<byte[], Task>>>();
        readonly List<Func<string, long, Task<long>>> followVersionHandlers = new List<Func<string, long, Task<long>>>();
        public Type GrainType { get; }

        public StringFollowUnit(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
            GrainType = grainType;
        }
        public static StringFollowUnit<E> From<Grain>(IServiceProvider serviceProvider)
            where Grain : Orleans.Grain
        {
            return new StringFollowUnit<E>(serviceProvider, typeof(Grain));
        }
        public List<Func<byte[], Task>> GetEventHandlers(string followType)
        {
            if (!eventHandlers.TryGetValue(followType, out var funcs))
            {
                funcs = new List<Func<byte[], Task>>();
                eventHandlers.Add(followType, funcs);
            }
            return funcs;
        }

        public List<Func<string, long, Task<long>>> GetAndSaveVersionFuncs()
        {
            return followVersionHandlers;
        }
        public StringFollowUnit<E> BindEventHandler(string followType, Func<byte[], Task> handler)
        {
            var funcs = GetEventHandlers(followType);
            funcs.Add(handler);
            return this;
        }
        public StringFollowUnit<E> BindFlow<F>(string followType)
            where F : IFollow, IGrainWithStringKey
        {
            var funcs = GetEventHandlers(followType);
            funcs.Add((byte[] bytes) =>
            {
                var (success, actorId) = EventBytesTransport.GetActorIdWithString(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).Tell(bytes);
                }
                return Task.CompletedTask;
            });
            followVersionHandlers.Add((stateId, version) => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).GetAndSaveVersion(version));
            return this;
        }

        public StringFollowUnit<E> BindConcurrentFlow<F>(string followType)
            where F : IConcurrentFollow, IGrainWithStringKey
        {
            var funcs = GetEventHandlers(followType);
            funcs.Add((byte[] bytes) =>
            {
                var (success, actorId) = EventBytesTransport.GetActorIdWithString(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).ConcurrentTell(bytes);
                }
                return Task.CompletedTask;
            });
            followVersionHandlers.Add((stateId, version) => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).GetAndSaveVersion(version));
            return this;
        }

        public List<Func<byte[], Task>> GetAllEventHandlers()
        {
            var list = new List<Func<byte[], Task>>();
            foreach (var values in eventHandlers.Values)
            {
                list.AddRange(values);
            }
            return list;
        }
    }
}
