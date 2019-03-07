using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitConsumer : Consumer
    {
        public RabbitConsumer(List<Func<byte[], Task>> eventHandlers) : base(eventHandlers)
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
