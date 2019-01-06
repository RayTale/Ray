using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;

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
        public RabbitConsumer<W> Bind<F>(Func<byte[], object, Task> handler)
            where F : IFollow
        {
            HandlerFuncs.Add(handler);
            return this;
        }
        public RabbitConsumer<W> BindFollowWithLongId<F>()
            where F : IFollow, IGrainWithIntegerKey
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEventBase<long> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.StateId).Tell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> BindConcurrentFollowWithLongId<F>()
            where F : IConcurrentFollow, IGrainWithIntegerKey
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEventBase<long> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.StateId).ConcurrentTell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> BindFollowWithStringId<F>()
            where F : IFollow, IGrainWithStringKey
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEventBase<string> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.StateId).Tell(bytes);
                else
                    return Task.CompletedTask;
            });
            return this;
        }
        public RabbitConsumer<W> BindConcurrentFollowWithStringId<F>()
            where F : IConcurrentFollow, IGrainWithStringKey
        {
            HandlerFuncs.Add((byte[] bytes, object evt) =>
            {
                if (evt is IEventBase<string> value)
                    return EventBus.ServiceProvider.GetService<IClusterClient>().GetGrain<F>(value.StateId).ConcurrentTell(bytes);
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
