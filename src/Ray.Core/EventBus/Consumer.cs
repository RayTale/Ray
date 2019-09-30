using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public abstract class Consumer : IConsumer
    {
        readonly List<Func<byte[], Task>> eventHandlers;
        readonly List<Func<List<byte[]>, Task>> batchEventHandlers;
        public Consumer(
            List<Func<byte[], Task>> eventHandlers,
            List<Func<List<byte[]>, Task>> batchEventHandlers)
        {
            this.eventHandlers = eventHandlers;
            this.batchEventHandlers = batchEventHandlers;
        }
        public void AddHandler(Func<byte[], Task> func)
        {
            eventHandlers.Add(func);
        }
        public Task Notice(byte[] bytes)
        {
            return Task.WhenAll(eventHandlers.Select(func => func(bytes)));
        }

        public Task Notice(List<byte[]> list)
        {
            return Task.WhenAll(batchEventHandlers.Select(func => func(list)));
        }
    }
}
