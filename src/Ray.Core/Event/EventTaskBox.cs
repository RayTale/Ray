namespace Ray.Core.Event
{
    public class EventTaskBox<PrimaryKey>
    {
        public EventTaskBox(FullyEvent<PrimaryKey> evt, string eventUtf8String, string uniqueId = null)
        {
            Event = evt;
            UniqueId = uniqueId;
            EventUtf8String = eventUtf8String;
        }
        public FullyEvent<PrimaryKey> Event { get; set; }
        public string EventUtf8String { get; set; }
        public string UniqueId { get; set; }
        public bool ReturnValue { get; set; }
    }
}
