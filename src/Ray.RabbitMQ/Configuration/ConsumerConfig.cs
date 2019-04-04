namespace Ray.EventBus.RabbitMQ
{
    public class ConsumerConfig
    {
        public ushort MinQos { get; set; }
        public ushort IncQos { get; set; }
        public ushort MaxQos { get; set; }
        public bool AutoAck { get; set; }
        public bool Reenqueue { get; set; }
    }
}
