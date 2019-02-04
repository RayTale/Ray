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
        public static void UpdateVersion<PrimaryKey>(this ISnapshot<PrimaryKey> snapshot, EventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<PrimaryKey>(this ISnapshot<PrimaryKey> snapshot, EventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0)
                snapshot.StartTimestamp = eventBase.Timestamp;
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
        public static string GetEventId<K>(this IFullyEvent<K> @event)
        {
            return $"{@event.StateId.ToString()}_{@event.Base.Version.ToString()}";
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static EventUID GetNextUID<K>(this IFullyEvent<K> @event)
        {
            return new EventUID(@event.GetEventId(), @event.Base.Timestamp);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeUpdateVersion<PrimaryKey>(this IFollowSnapshot<PrimaryKey> snapshot, EventBase eventBase)
        {
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<K>(this IFollowSnapshot<K> state, Type grainType)
        {
            if (state.DoingVersion != state.Version)
                throw new StateInsecurityException(state.StateId.ToString(), grainType, state.DoingVersion, state.Version);
            state.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<PrimaryKey>(this IFollowSnapshot<PrimaryKey> snapshot, EventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<PrimaryKey>(this IFollowSnapshot<PrimaryKey> snapshot, EventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
    }
}
