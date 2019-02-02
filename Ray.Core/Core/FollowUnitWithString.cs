using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;

namespace Ray.Core
{
    public class FollowUnitWithString<E> : IFollowUnit<string>
    {
        readonly IServiceProvider serviceProvider;
        readonly List<Func<byte[], Task>> eventHandlers = new List<Func<byte[], Task>>();
        readonly List<Func<string, Task<long>>> followVersionHandlers = new List<Func<string, Task<long>>>();
        public Type GrainType { get; }

        public FollowUnitWithString(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
            GrainType = grainType;
        }
        public static FollowUnitWithString<E> From<Grain>(IServiceProvider serviceProvider)
            where Grain : Orleans.Grain
        {
            return new FollowUnitWithString<E>(serviceProvider, typeof(Grain));
        }
        public List<Func<byte[], Task>> GetEventHandlers()
        {
            return eventHandlers;
        }

        public List<Func<string, Task<long>>> GetAllVersionsFunc()
        {
            return followVersionHandlers;
        }
        public FollowUnitWithString<E> BindEventHandler(Func<byte[], Task> handler)
        {
            eventHandlers.Add(handler);
            return this;
        }
        public FollowUnitWithString<E> BindFlow<F>()
            where F : IFollow, IGrainWithStringKey
        {
            eventHandlers.Add((byte[] bytes) =>
            {
                var (success, actorId) = BytesTransport.GetActorIdWithString(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).Tell(bytes);
                }
                return Task.CompletedTask;
            });
            followVersionHandlers.Add(stateId => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).CurrentVersion());
            return this;
        }

        public FollowUnitWithString<E> BindConcurrentFlow<F>()
            where F : IConcurrentFollow, IGrainWithStringKey
        {
            eventHandlers.Add((byte[] bytes) =>
            {
                var (success, actorId) = BytesTransport.GetActorIdWithString(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).ConcurrentTell(bytes);
                }
                return Task.CompletedTask;
            });
            followVersionHandlers.Add(stateId => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).CurrentVersion());
            return this;
        }
    }
}
