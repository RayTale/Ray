using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public abstract class Consumer : IConsumer
    {
        readonly List<Func<BytesBox, Task>> eventHandlers;
        readonly List<Func<List<BytesBox>, Task>> batchEventHandlers;
        public Consumer(
            List<Func<BytesBox, Task>> eventHandlers,
            List<Func<List<BytesBox>, Task>> batchEventHandlers)
        {
            this.eventHandlers = eventHandlers;
            this.batchEventHandlers = batchEventHandlers;
        }
        public void AddHandler(Func<BytesBox, Task> func)
        {
            eventHandlers.Add(func);
        }
        public Task Notice(BytesBox bytes)
        {
            return Task.WhenAll(eventHandlers.Select(func => func(bytes)));
        }

        public Task Notice(List<BytesBox> list)
        {
            return Task.WhenAll(batchEventHandlers.Select(func => func(list)));
        }
    }
}
