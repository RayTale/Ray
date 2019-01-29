using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.EventBus;
using Ray.Core.Serialization;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitConsumer : Consumer
    {
        public RabbitConsumer(
            IServiceProvider serviceProvider,
            List<Func<byte[], object, Task>> eventHandlers,
            ISerializer serializer) :
            base(serviceProvider, eventHandlers, serializer)
        {
        }
        public RabbitEventBus EventBus { get; set; }
        public List<QueueInfo> QueueList { get; set; }
        public ushort MinQos { get; set; }
        public ushort IncQos { get; set; }
        public ushort MaxQos { get; set; }
        public bool AutoAck { get; set; }
        public bool ErrorReject { get; set; }
    }
}
