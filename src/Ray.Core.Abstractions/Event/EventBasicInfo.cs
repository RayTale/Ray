using Orleans.Concurrency;

namespace Ray.Core.Event
{
    [Immutable]
    public class EventBasicInfo
    {
        public EventBasicInfo() { }
        public EventBasicInfo(long version, long timestamp)
        {
            Version = version;
            Timestamp = timestamp;
        }
        public long Version { get; set; }
        public long Timestamp { get; set; }
    }
}
