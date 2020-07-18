namespace Ray.Core.Event
{
    /// <summary>
    /// A typed wrapper for an event that contains details about the event.
    /// </summary>
    /// <typeparam name="TPrimaryKey">The type of the entity's key.</typeparam>
    public class FullyEvent<TPrimaryKey>
    {
        public IEvent Event { get; set; }
        public EventBasicInfo BasicInfo { get; set; }
        public TPrimaryKey StateId { get; set; }
    }
}