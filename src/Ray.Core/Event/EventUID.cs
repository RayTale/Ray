using System;
using Orleans.Concurrency;

namespace Ray.Core.Event
{
    [Immutable]
    public class EventUID
    {
        public static readonly EventUID Empty = new EventUID(null, DateTimeOffset.MinValue.ToUnixTimeMilliseconds());
        public EventUID(string uid, long timestamp)
        {
            UID = uid;
            Timestamp = timestamp;
        }
        public string UID { get; }
        public long Timestamp { get; }
    }
}
