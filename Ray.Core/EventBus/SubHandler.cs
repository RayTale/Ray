using System;
using System.Threading.Tasks;
using System.IO;
using Ray.Core.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.EventBus
{
    public abstract class SubHandler<TMessageWrapper> : ISubHandler
        where TMessageWrapper : IMessageWrapper
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
                if (!TypeContainer.TryGetValue(msg.TypeName, out var type))
                {
                    throw new Exception($"{ msg.TypeName } does not exist");
                }
                using (var ems = new MemoryStream(msg.Bytes))
                {
                    return Notice(bytes, msg.Bytes, msg, serializer.Deserialize(type, ems));
                }
            }
        }

        public abstract Task Tell(byte[] wrapBytes, byte[] dataBytes, object data, TMessageWrapper msg);

    }
}
