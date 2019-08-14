using System;

namespace Ray.EventBus.RabbitMQ
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ProducerAttribute : Attribute
    {
        public ProducerAttribute(string exchange = null, string routePrefix = null, int lBCount = 1, ushort minQos = 100, ushort incQos = 100, ushort maxQos = 300, bool autoAck = false, bool reenqueue = false)
        {
            Exchange = exchange;
            RoutePrefix = routePrefix;
            LBCount = lBCount;
            AutoAck = autoAck;
            MaxQos = maxQos;
            MinQos = minQos;
            IncQos = incQos;
            Reenqueue = reenqueue;
        }
        public string Exchange { get; }
        public string RoutePrefix { get; }
        public int LBCount { get; }
        public ushort MinQos { get; set; }
        public ushort IncQos { get; set; }
        public ushort MaxQos { get; set; }
        public bool AutoAck { get; set; }
        public bool Reenqueue { get; set; }
    }
}
