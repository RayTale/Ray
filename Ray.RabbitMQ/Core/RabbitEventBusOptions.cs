namespace Ray.EventBus.RabbitMQ
{
    public class RabbitEventBusOptions
    {
        public string[] Nodes { get; set; }
        public int QueueStartMillisecondsDelay { get; set; } = 10;
    }
}
