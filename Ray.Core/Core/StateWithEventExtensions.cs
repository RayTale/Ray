using System;
using System.Runtime.CompilerServices;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.State;

namespace Ray.Core
{
    public static class StateWithEventExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<K>(this ISnapshot<K> snapshot, IEvent<K> @event, Type grainType)
        {
            var eventBase = @event.GetBase();
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.Version = eventBase.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<K>(this ISnapshot<K> snapshot, IEvent<K> @event, Type grainType)
        {
            var eventBase = @event.GetBase();
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeUpdateVersion<K>(this ISnapshot<K> snapshot, long version)
        {
            snapshot.DoingVersion = version;
            snapshot.Version = version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<K>(this ISnapshot<K> snapshot, Type grainType)
        {
            if (snapshot.DoingVersion != snapshot.Version)
                throw new StateInsecurityException(snapshot.StateId.ToString(), grainType, snapshot.DoingVersion, snapshot.Version);
            snapshot.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DecrementDoingVersion<K>(this ISnapshot<K> snapshot)
        {
            snapshot.DoingVersion -= 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetEventId<K>(this IEvent<K> @event)
        {
            var eventBase = @event.GetBase();
            return $"{eventBase.StateId.ToString()}_{eventBase.Version.ToString()}";
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static EventUID GetNextUID<K>(this IEvent<K> @event)
        {
            return new EventUID(@event.GetEventId(), @event.GetBase().Timestamp);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeUpdateVersion<K>(this IFollowSnapshot<K> state, long version)
        {
            state.DoingVersion = version;
            state.Version = version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<K>(this IFollowSnapshot<K> state, Type grainType)
        {
            if (state.DoingVersion != state.Version)
                throw new StateInsecurityException(state.StateId.ToString(), grainType, state.DoingVersion, state.Version);
            state.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<K>(this IFollowSnapshot<K> state, IEvent<K> @event, Type grainType)
        {
            var eventBase = @event.GetBase();
            if (state.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(state.StateId.ToString(), grainType, eventBase.Version, state.Version);
            state.Version = eventBase.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<K>(this IFollowSnapshot<K> state, IEvent<K> @event, Type grainType)
        {
            var eventBase = @event.GetBase();
            if (state.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(state.StateId.ToString(), grainType, eventBase.Version, state.Version);
            state.DoingVersion = eventBase.Version;
            state.Version = eventBase.Version;
        }
    }
}
