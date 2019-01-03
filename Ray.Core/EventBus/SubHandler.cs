using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.Messaging;

namespace Ray.Core.EventBus
{
    public abstract class SubHandler<TMessageWrapper> : ISubHandler
        where TMessageWrapper : IBytesWrapper
    {
        readonly IServiceProvider serviceProvider;
        public SubHandler(IServiceProvider svProvider)
        {
            serviceProvider = svProvider;
        }
        public virtual Task Notice(byte[] wrapBytes, byte[] dataBytes, TMessageWrapper message, object data)
        {
            return Tell(wrapBytes, dataBytes, data, message);
        }
        public Task Notice(byte[] bytes)
        {
            var serializer = serviceProvider.GetService<ISerializer>();
            using (var ms = new MemoryStream(bytes))
            {
                var msg = serializer.Deserialize<TMessageWrapper>(ms);
                using (var ems = new MemoryStream(msg.Bytes))
                {
                    return Notice(bytes, msg.Bytes, msg, serializer.Deserialize(TypeContainer.GetType(msg.TypeName), ems));
                }
            }
        }

        public abstract Task Tell(byte[] wrapBytes, byte[] dataBytes, object data, TMessageWrapper msg);

    }
}
