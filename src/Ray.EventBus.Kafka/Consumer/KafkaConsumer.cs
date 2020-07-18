using Ray.Core.EventBus;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public class KafkaConsumer : Consumer
    {
        public KafkaConsumer(
            List<Func<BytesBox, Task>> eventHandlers,
            List<Func<List<BytesBox>, Task>> batchEventHandlers) : base(eventHandlers, batchEventHandlers)
        {
        }
        public KafkaEventBus EventBus { get; set; }
        public ConsumerOptions Config { get; set; }
        public List<string> Topics { get; set; }
        public string Group { get; set; }
    }
}
