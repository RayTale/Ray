using Orleans.Concurrency;

namespace Ray.Core.Event
{
    [Immutable]
    public class EventBasicInfo
    {
        public EventBasicInfo()
        {
        }

        public EventBasicInfo(long version, long timestamp)
        {
            this.Version = version;
            this.Timestamp = timestamp;
        }

        public long Version { get; set; }

        public long Timestamp { get; set; }
    }
}
