namespace Ray.Core.Event
{
    public interface IEventBase
    {
        long Version { get; set; }
        long Timestamp { get; set; }
    }
}
