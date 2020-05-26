namespace Ray.Core.Event
{
    public class FullyEvent<PrimaryKey>
    {
        public IEvent Event { get; set; }
        public EventBasicInfo BasicInfo { get; set; }
        public PrimaryKey ActorId { get; set; }
    }
}
