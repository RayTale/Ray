using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;

namespace Ray.Core
{
    public class FollowUnitWithLong<E> : IFollowUnit<long>
        where E : IEventBase<long>
    {
        readonly IServiceProvider serviceProvider;
        readonly List<Func<byte[], object, Task>> eventHandlers = new List<Func<byte[], object, Task>>();
        readonly List<Func<long, Task<long>>> followVersionHandlers = new List<Func<long, Task<long>>>();

        public Type GrainType { get; }

        public FollowUnitWithLong(IServiceProvider serviceProvider, Type grainType)
        {
            this.serviceProvider = serviceProvider;
            GrainType = grainType;
        }
        public static FollowUnitWithLong<E> From<Grain>(IServiceProvider serviceProvider)
            where Grain : Orleans.Grain
        {
            return new FollowUnitWithLong<E>(serviceProvider, typeof(Grain));
        }
        public List<Func<byte[], object, Task>> GetEventHandlers()
        {
            return eventHandlers;
        }

        public Func<long, Task<long[]>> GetAllVersionsFunc()
        {
            return stateId => Task.WhenAll(followVersionHandlers.Select(m => m(stateId)));
        }
        public FollowUnitWithLong<E> BindEventHandler(Func<byte[], object, Task> handler)
        {
            eventHandlers.Add(handler);
            return this;
        }
        public FollowUnitWithLong<E> BindFlow<F>()
            where F : IFollow, IGrainWithIntegerKey

        {
            eventHandlers.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEvent<long, E> value)
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(value.Base.StateId).Tell(bytes);
                else
                    return Task.CompletedTask;
            });
            followVersionHandlers.Add(stateId => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).CurrentVersion());
            return this;
        }

        public FollowUnitWithLong<E> BindConcurrentFlow<F>()
            where F : IConcurrentFollow, IGrainWithIntegerKey
        {
            eventHandlers.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEvent<long, E> value)
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(value.Base.StateId).ConcurrentTell(bytes);
                else
                    return Task.CompletedTask;
            });
            followVersionHandlers.Add(stateId => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).CurrentVersion());
            return this;
        }
    }
}
