namespace Ray.Core.Event
{
    public class FullyEvent<PrimaryKey>
    {
        public IEvent Event { get; set; }
        public EventBase Base { get; set; }
        public PrimaryKey StateId { get; set; }
    }
}
