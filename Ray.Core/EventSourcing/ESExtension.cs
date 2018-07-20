using System;
using System.Runtime.CompilerServices;

namespace Ray.Core.EventSourcing
{
    public static class ESExtension
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<K>(this IState<K> state, IEventBase<K> @event)
        {
            if (state.Version + 1 != @event.Version)
                throw new Exception($"Event version and state version don't match!,Event Version={@event.Version},State Version={state.Version}");
            state.Version = @event.Version;
            state.VersionTime = @event.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<K>(this IState<K> state, long version, DateTime time)
        {
            state.DoingVersion = version;
            state.Version = version;
            state.VersionTime = time;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<K>(this IState<K> state)
        {
            if (state.DoingVersion != state.Version)
                throw new Exception($"State doing version with state version don't match!,doing Version={state.DoingVersion},State Version={state.Version}");
            state.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DecrementDoingVersion<K>(this IState<K> state)
        {
            state.DoingVersion -= 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetUniqueId<K>(this IEventBase<K> @event)
        {
            return @event.Version.ToString();
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetEventId<K>(this IEventBase<K> @event)
        {
            return $"{@event.StateId}_{@event.Version}";
        }
    }
}
