using System;
using Orleans.Concurrency;

namespace Ray.Core.Event
{
    [Immutable]
    public class EventUID
    {
        public EventUID()
        {
        }
        public EventUID(string uid, long timestamp)
        {
            if (string.IsNullOrWhiteSpace(uid))
                throw new ArgumentNullException(nameof(uid));

            UID = uid;
            Timestamp = timestamp;
        }
        public string UID { get; set; }
        public long Timestamp { get; set; }
    }
}
