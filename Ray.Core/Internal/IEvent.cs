using System;
using Ray.Core.Messaging;

namespace Ray.Core.Internal
{
    public interface IEvent
    {
        long Version { get; set; }
        DateTime Timestamp { get; set; }
    }

    public interface IEventBase<K> : IEvent, IStateMessage<K>
    {
    }
}
