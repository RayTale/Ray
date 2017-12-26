using System;
using System.Threading.Tasks;
using System.IO;
using Ray.Core.Message;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.MQ
{
    public abstract class AllSubHandler<K, TMessageWrapper> : ISubHandler where TMessageWrapper : MessageWrapper
    {
        public virtual async Task Notice(byte[] dataBytes, TMessageWrapper message, object data)
        {
            if (data is IActorOwnMessage<K> @event)
                await Tell(dataBytes, @event, message);
        }
        public Task Notice(byte[] bytes)
        {
            var serializer = Global.IocProvider.GetService<ISerializer>();
            using (var ms = new MemoryStream(bytes))
            {
                var msg = serializer.Deserialize<TMessageWrapper>(ms);
                var type = MessageTypeMapping.GetType(msg.TypeCode);
                if (type == null)
                {
                    throw new Exception("TypeCode为" + msg.TypeCode + "的Type不存在");
                }
                using (var ems = new MemoryStream(msg.BinaryBytes))
                {
                    return this.Notice(bytes, msg, serializer.Deserialize(type, ems));
                }
            }
        }

        public abstract Task Tell(byte[] dataBytes, IActorOwnMessage<K> data, TMessageWrapper msg);

    }
}
