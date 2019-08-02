namespace Ray.EventBus.Kafka
{
    public class KafkaEventBusOptions
    {
        public string[] Nodes { get; set; }
        public int QueueStartMillisecondsDelay { get; set; } = 10;
    }
}
