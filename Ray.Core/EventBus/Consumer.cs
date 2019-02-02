using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public abstract class Consumer : IConsumer
    {
        readonly List<Func<byte[], Task>> eventHandlers;
        public Consumer(List<Func<byte[], Task>> eventHandlers)
        {
            this.eventHandlers = eventHandlers;
        }
        public Task Notice(byte[] bytes)
        {
            return Task.WhenAll(eventHandlers.Select(func => func(bytes)));
        }
    }
}
