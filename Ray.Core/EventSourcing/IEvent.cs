using Ray.Core.Message;
using System;

namespace Ray.Core.EventSourcing
{
    public interface IEvent : IMessage
    {
        string Id { get; set; }
        UInt32 Version { get; set; }
        string CommandId { get; set; }
        DateTime Timestamp { get; set; }
    }

    public interface IEventBase<K> : IEvent, IActorOwnMessage<K>
    {
        void Apply(IState<K> state);
    }
}
