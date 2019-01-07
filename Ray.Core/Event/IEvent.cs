using System;

namespace Ray.Core.Event
{
    public interface IEvent
    {
        long Version { get; set; }
        DateTime Timestamp { get; set; }
    }
}
