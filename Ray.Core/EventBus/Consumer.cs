using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Ray.Core.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.EventBus
{
    public abstract class Consumer : IConsumer
    {
        readonly ISerializer serializer;
        readonly List<Func<byte[], object, Task>> eventHandlers;
        readonly Type bytesWrapper;
        public Consumer(IServiceProvider serviceProvider, List<Func<byte[], object, Task>> eventHandlers, ISerializer serializer)
        {
            bytesWrapper = serviceProvider.GetService<IBytesWrapper>().GetType();
            this.eventHandlers = eventHandlers;
            this.serializer = serializer;
        }
        public Task Notice(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            {
                var data = serializer.Deserialize(bytesWrapper, ms);
                if (data is IBytesWrapper msg)
                {
                    using (var ems = new MemoryStream(msg.Bytes))
                    {
                        var evt = serializer.Deserialize(TypeContainer.GetType(msg.TypeName), ems);
                        return Task.WhenAll(eventHandlers.Select(func => func(bytes, evt)));
                    }
                }
                return Task.CompletedTask;
            }
        }
    }
}
