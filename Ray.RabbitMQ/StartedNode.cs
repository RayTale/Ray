namespace Ray.EventBus.RabbitMQ
{
    public class StartedNode
    {
        public string Node { get; set; }
        public long LockId { get; set; }
    }
}
