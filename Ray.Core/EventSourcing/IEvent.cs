using Ray.Core.Message;
using System;

namespace Ray.Core.EventSourcing
{
    public interface IEvent : IMessage
    {
        Int64 Version { get; set; }
        DateTime Timestamp { get; set; }
    }

    public interface IEventBase<K> : IEvent, IActorOwnMessage<K>
    {
    }
}
