using System;

namespace Ray.EventBus.Kafka
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ProducerAttribute : Attribute
    {
        public ProducerAttribute(string topic, int lBCount = 1)
        {
            Topic = topic;
            LBCount = lBCount;
        }
        public string Topic { get; }
        public int LBCount { get; }
    }
}
