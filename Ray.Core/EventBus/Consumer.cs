using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Ray.Core.Serialization;

namespace Ray.Core.EventBus
{
    public abstract class Consumer<W> : IConsumer
        where W : IBytesWrapper
    {
        readonly ISerializer serializer;
        readonly List<Func<byte[], object, Task>> eventHandlers;
        public Consumer(List<Func<byte[], object, Task>> eventHandlers, ISerializer serializer)
        {
            this.eventHandlers = eventHandlers;
            this.serializer = serializer;
        }
        public Task Notice(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            {
                var msg = serializer.Deserialize<W>(ms);
                using (var ems = new MemoryStream(msg.Bytes))
                {
                    return Task.WhenAll(eventHandlers.Select(func => func(bytes, serializer.Deserialize(TypeContainer.GetType(msg.TypeName), ems))));
                }
            }
        }
    }
}
