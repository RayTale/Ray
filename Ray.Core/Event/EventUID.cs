using System;

namespace Ray.Core.Event
{
    public class EventUID
    {
        public static readonly EventUID Empty = new EventUID(null, DateTime.MinValue);
        public EventUID(string uid, DateTime timestamp)
        {
            UID = uid;
            Timestamp = timestamp;
        }
        public string UID { get; }
        public DateTime Timestamp { get; }
    }
}
