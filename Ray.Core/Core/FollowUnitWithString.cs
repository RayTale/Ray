using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;

namespace Ray.Core
{
    public class FollowUnitWithString<E> : IFollowUnit<string>
        where E : IEventBase<string>
    {
        readonly IServiceProvider serviceProvider;
        readonly List<Func<byte[], object, Task>> eventHandlers = new List<Func<byte[], object, Task>>();
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
        public List<Func<byte[], object, Task>> GetEventHandlers()
        {
            return eventHandlers;
        }

        public List<Func<string, Task<long>>> GetAllVersionsFunc()
        {
            return followVersionHandlers;
        }
        public FollowUnitWithString<E> BindEventHandler(Func<byte[], object, Task> handler)
        {
            eventHandlers.Add(handler);
            return this;
        }
        public FollowUnitWithString<E> BindFlow<F>()
            where F : IConcurrentFollow, IGrainWithStringKey
        {
            eventHandlers.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEvent<string> value)
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(value.GetBase().StateId).ConcurrentTell(bytes);
                else
                    return Task.CompletedTask;
            });
            followVersionHandlers.Add(stateId => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).CurrentVersion());
            return this;
        }

        public FollowUnitWithString<E> BindConcurrentFlow<F>()
            where F : IConcurrentFollow, IGrainWithStringKey
        {
            eventHandlers.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEvent<string> value)
                    return serviceProvider.GetService<IClusterClient>().GetGrain<F>(value.GetBase().StateId).ConcurrentTell(bytes);
                else
                    return Task.CompletedTask;
            });
            followVersionHandlers.Add(stateId => serviceProvider.GetService<IClusterClient>().GetGrain<F>(stateId).CurrentVersion());
            return this;
        }
    }
}
