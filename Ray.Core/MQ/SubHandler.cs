using System;
using System.Threading.Tasks;
using System.IO;
using Ray.Core.Message;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.MQ
{
    public abstract class SubHandler<K, TMessageWrapper> : ISubHandler 
        where TMessageWrapper : MessageWrapper
    {
        IServiceProvider serviceProvider;
        public SubHandler(IServiceProvider svProvider)
        {
            this.serviceProvider = svProvider;
        }
        public virtual async Task Notice(byte[] bytes, TMessageWrapper message, object data)
        {
            if (data is IActorOwnMessage<K> @event)
                await Tell(bytes, @event, message);
        }
        public Task Notice(byte[] bytes)
        {
            var serializer = serviceProvider.GetService<ISerializer>();
            using (var ms = new MemoryStream(bytes))
            {
                var msg = serializer.Deserialize<TMessageWrapper>(ms);
                var type = MessageTypeMapper.GetType(msg.TypeCode);
                if (type == null)
                {
                    throw new Exception($"TypeCode for { msg.TypeCode } type does not exist");
                }
                using (var ems = new MemoryStream(msg.BinaryBytes))
                {
                    return Notice(bytes, msg, serializer.Deserialize(type, ems));
                }
            }
        }

        public abstract Task Tell(byte[] bytes, IActorOwnMessage<K> data, TMessageWrapper msg);

    }
}
