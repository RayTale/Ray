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
        readonly List<Func<byte[], Task>> eventHandlers = new List<Func<byte[], Task>>();
        readonly List<Func<long, Task<long>>> followVersionHandlers = new List<Func<long, Task<long>>>();

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
        public List<Func<byte[], Task>> GetEventHandlers()
        {
            return eventHandlers;
        }

        public List<Func<long, Task<long>>> GetAllVersionsFunc()
        {
            return followVersionHandlers;
        }
        public FollowUnitWithLong BindEventHandler(Func<byte[], Task> handler)
        {
            eventHandlers.Add(handler);
            return this;
        }
        public FollowUnitWithLong BindFlow<F>()
            where F : IFollow, IGrainWithIntegerKey

        {
            eventHandlers.Add((byte[] bytes) =>
            {
                var (success, actorId) = BytesTransport.GetActorIdWithLong(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).Tell(bytes);
                }
                return Task.CompletedTask;
            });
            followVersionHandlers.Add(stateId => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).CurrentVersion());
            return this;
        }

        public FollowUnitWithLong BindConcurrentFlow<F>()
            where F : IConcurrentFollow, IGrainWithIntegerKey
        {
            eventHandlers.Add((byte[] bytes) =>
            {
                var (success, actorId) = BytesTransport.GetActorIdWithLong(bytes);
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
