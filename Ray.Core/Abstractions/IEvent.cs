using System;

namespace Ray.Core.Abstractions
{
    public interface IEvent
    {
        long Version { get; set; }
        DateTime Timestamp { get; set; }
    }
}
