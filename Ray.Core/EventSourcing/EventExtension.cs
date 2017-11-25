using System;

namespace Ray.Core.EventSourcing
{
    public static class EventExtension
    {
        public static void ApplyBase<K>(this IEventBase<K> @event, IState<K> state)
        {
            if (state.Version + 1 != @event.Version)
                throw new Exception($"Event version and state version don't match!,Event Version={@event.Version},State Version={state.Version}");
            state.Version = @event.Version;
            state.VersionTime = @event.Timestamp;
        }
    }
}
