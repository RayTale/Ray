using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public abstract class Consumer : IConsumer
    {
        private readonly List<Func<BytesBox, Task>> eventHandlers;
        private readonly List<Func<List<BytesBox>, Task>> batchEventHandlers;

        public Consumer(
            List<Func<BytesBox, Task>> eventHandlers,
            List<Func<List<BytesBox>, Task>> batchEventHandlers)
        {
            this.eventHandlers = eventHandlers;
            this.batchEventHandlers = batchEventHandlers;
        }

        public void AddHandler(Func<BytesBox, Task> func)
        {
            this.eventHandlers.Add(func);
        }

        public Task Notice(BytesBox bytes)
        {
            return Task.WhenAll(this.eventHandlers.Select(func => func(bytes)));
        }

        public Task Notice(List<BytesBox> list)
        {
            return Task.WhenAll(this.batchEventHandlers.Select(func => func(list)));
        }
    }
}
