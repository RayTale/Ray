using Ray.Core.EventBus;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public class KafkaConsumer : Consumer
    {
        public KafkaConsumer(List<Func<byte[], Task>> eventHandlers) : base(eventHandlers)
        {
        }
        public KafkaEventBus EventBus { get; set; }
        public List<string> Topics { get; set; }
        public string Group { get; set; }
    }
}
