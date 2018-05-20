using System;
using System.Threading.Tasks;
using System.IO;
using Ray.Core.Message;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.MQ
{
    public abstract class SubHandler<TMessageWrapper> : ISubHandler
        where TMessageWrapper : MessageWrapper
    {
        IServiceProvider serviceProvider;
        public SubHandler(IServiceProvider svProvider)
        {
            serviceProvider = svProvider;
        }
        public virtual async Task Notice(byte[] bytes, TMessageWrapper message, object data)
        {
            if (data is IMessage msgData)
                await Tell(bytes, msgData, message);
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
                    throw new Exception($"{ msg.TypeCode } does not exist");
                }
                using (var ems = new MemoryStream(msg.BinaryBytes))
                {
                    return Notice(bytes, msg, serializer.Deserialize(type, ems));
                }
            }
        }

        public abstract Task Tell(byte[] bytes, IMessage data, TMessageWrapper msg);

    }
}
