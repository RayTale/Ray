using System.IO;
using System.Threading.Tasks;
using Ray.Core.Abstractions;
using Ray.Core.Messaging;

namespace Ray.Core.EventBus
{
    public abstract class Consumer<W> : IConsumer
        where W : IBytesWrapper
    {
        readonly ISerializer serializer;
        public Consumer(ISerializer serializer)
        {
            this.serializer = serializer;
        }
        public Task Notice(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            {
                var msg = serializer.Deserialize<W>(ms);
                using (var ems = new MemoryStream(msg.Bytes))
                {
                    return Handler(bytes, serializer.Deserialize(TypeContainer.GetType(msg.TypeName), ems));
                }
            }
        }
        public abstract Task Handler(byte[] bytes, object data);
    }
}
