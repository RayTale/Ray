using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;
using Ray.Core.Serialization;

namespace Ray.Core
{
    public class FollowUnitWithLong : IFollowUnit<long>
    {
        readonly IServiceProvider serviceProvider;
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
        public List<Func<byte[], Task>> GetEventHandlers()
        {
            return eventHandlers;
        }

        public List<Func<long, long, Task<long>>> GetAndSaveVersionFuncs()
        {
            return followVersionHandlers;
        }
        public FollowUnitWithLong BindEventHandler(Func<byte[], Task> handler)
        {
            eventHandlers.Add(handler);
            return this;
        }
        public FollowUnitWithLong Flow<F>()
            where F : IFollow, IGrainWithIntegerKey

        {
            eventHandlers.Add((byte[] bytes) =>
            {
                var (success, actorId) = EventBytesTransport.GetActorIdWithLong(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).Tell(bytes);
                }
                return Task.CompletedTask;
            });
            followVersionHandlers.Add((stateId, version) => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).GetAndSaveVersion(version));
            return this;
        }

        public FollowUnitWithLong ConcurrentFlow<F>()
            where F : IConcurrentFollow, IGrainWithIntegerKey
        {
            eventHandlers.Add((byte[] bytes) =>
            {
                var (success, actorId) = EventBytesTransport.GetActorIdWithLong(bytes);
                if (success)
                {
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(actorId).ConcurrentTell(bytes);
                }
                return Task.CompletedTask;
            });
            followVersionHandlers.Add((stateId, version) => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).GetAndSaveVersion(version));
            return this;
        }
    }
}
