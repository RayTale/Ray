using System;

namespace Ray.EventBus.Kafka
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ProducerAttribute : Attribute
    {
        public ProducerAttribute(string topic = default, int lBCount = 1, bool reenqueue = true)
        {
            Topic = topic;
            LBCount = lBCount;
            Reenqueue = reenqueue;
        }
        public string Topic { get; }
        public int LBCount { get; }
        public bool Reenqueue { get; }
    }
}
