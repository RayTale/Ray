using System;
using System.Runtime.CompilerServices;
using Ray.Core.Abstractions;
using Ray.Core.Exceptions;

namespace Ray.Core.Internal
{
    public static class VersionExtension
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<K>(this IState<K> state, IEvent @event, Type grainType)
        {
            if (state.Version + 1 != @event.Version)
                throw new EventVersionNotMatchStateException(state.StateId.ToString(), grainType, @event.Version, state.Version);
            state.Version = @event.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<K>(this IState<K> state, IEvent @event, Type grainType)
        {
            if (state.Version + 1 != @event.Version)
                throw new EventVersionNotMatchStateException(state.StateId.ToString(), grainType, @event.Version, state.Version);
            state.DoingVersion = @event.Version;
            state.Version = @event.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeUpdateVersion<K>(this IState<K> state, long version, DateTime time)
        {
            state.DoingVersion = version;
            state.Version = version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<K>(this IState<K> state, Type grainType)
        {
            if (state.DoingVersion != state.Version)
                throw new StateInsecurityException(state.StateId.ToString(), grainType, state.DoingVersion, state.Version);
            state.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DecrementDoingVersion<K>(this IState<K> state)
        {
            state.DoingVersion -= 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetUniqueId(this IEvent @event)
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
