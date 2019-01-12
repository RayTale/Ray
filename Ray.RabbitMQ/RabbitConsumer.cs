using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core;
using Ray.Core.Event;
using Ray.Core.EventBus;
using Ray.Core.Serialization;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitConsumer<W> : Consumer<W>
            where W : IBytesWrapper
    {
        public RabbitConsumer(ISerializer serializer) : base(serializer)
        {
        }
        public RabbitEventBus<W> EventBus { get; set; }
        public List<QueueInfo> QueueList { get; set; }
        public ushort MinQos { get; set; }
        public ushort IncQos { get; set; }
        public ushort MaxQos { get; set; }
        public bool AutoAck { get; set; }
        public bool ErrorReject { get; set; }
        public List<Func<byte[], object, Task>> HandlerFuncs { get; set; }

        public override Task Handler(byte[] bytes, object data)
        {
            return Task.WhenAll(HandlerFuncs.Select(func => func(bytes, data)));
        }
        public RabbitConsumer<W> Post<F>(Func<byte[], object, Task> handler)
            where F : IFollow
        {
            HandlerFuncs.Add(handler);
            return this;
        }
        public RabbitConsumer<W> PostWithLongID<F, E>()
            where F : IFollow, IGrainWithIntegerKey
            where E : IEventBase<long>
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEvent<long, E> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.Base.StateId).Tell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> ConcurrentPostWithLongID<F, E>()
            where F : IConcurrentFollow, IGrainWithIntegerKey
            where E : IEventBase<long>
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEvent<long, E> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.Base.StateId).ConcurrentTell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> PostWithStringID<F, E>()
            where F : IFollow, IGrainWithStringKey
            where E : IEventBase<string>
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEvent<string, E> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.Base.StateId).Tell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> ConcurrentPostWithStringID<F, E>()
            where F : IConcurrentFollow, IGrainWithStringKey
            where E : IEventBase<string>
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEvent<string, E> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.Base.StateId).ConcurrentTell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitEventBus<W> Complete()
        {
            return EventBus;
        }
    }
}
