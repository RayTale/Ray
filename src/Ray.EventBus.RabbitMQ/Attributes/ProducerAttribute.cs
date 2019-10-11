using System;

namespace Ray.EventBus.RabbitMQ
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ProducerAttribute : Attribute
    {
        public ProducerAttribute(string exchange = null, string routePrefix = null, int lBCount = 1,  bool autoAck = false, bool reenqueue = false, bool persistent = false)
        {
            Exchange = exchange;
            RoutePrefix = routePrefix;
            LBCount = lBCount;
            AutoAck = autoAck;
            Reenqueue = reenqueue;
            Persistent = persistent;
        }
        public string Exchange { get; }
        public string RoutePrefix { get; }
        public int LBCount { get; }
        public bool AutoAck { get; set; }
        public bool Reenqueue { get; set; }
        public bool Persistent { get; set; }
    }
}
