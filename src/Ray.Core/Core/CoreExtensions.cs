using System;
using System.Runtime.CompilerServices;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Snapshot;

namespace Ray.Core
{
    public static class CoreExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<PrimaryKey>(this ISnapshotBase<PrimaryKey> snapshot, EventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionUnorderedException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
            if (eventBase.Timestamp < snapshot.LatestMinEventTimestamp)
                snapshot.LatestMinEventTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<PrimaryKey>(this ISnapshotBase<PrimaryKey> snapshot, EventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionUnorderedException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
            if (eventBase.Timestamp < snapshot.LatestMinEventTimestamp)
                snapshot.LatestMinEventTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<PrimaryKey>(this ISnapshotBase<PrimaryKey> snapshot, Type grainType)
        {
            if (snapshot.DoingVersion != snapshot.Version)
                throw new StateInsecurityException(snapshot.StateId.ToString(), grainType, snapshot.DoingVersion, snapshot.Version);
            snapshot.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DecrementDoingVersion<PrimaryKey>(this ISnapshotBase<PrimaryKey> snapshot)
        {
            snapshot.DoingVersion -= 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetEventId(this EventBase eventBase, string stateId)
        {
            return $"{stateId}_{eventBase.Version.ToString()}";
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetEventId<PrimaryKey>(this IFullyEvent<PrimaryKey> @event)
        {
            return $"{@event.StateId.ToString()}_{@event.Base.Version.ToString()}";
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static EventUID GetNextUID<PrimaryKey>(this IFullyEvent<PrimaryKey> @event)
        {
            return new EventUID(@event.GetEventId(), @event.Base.Timestamp);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeUpdateVersion<PrimaryKey>(this IObserverSnapshot<PrimaryKey> snapshot, EventBase eventBase)
        {
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<PrimaryKey>(this IObserverSnapshot<PrimaryKey> state, Type grainType)
        {
            if (state.DoingVersion != state.Version)
                throw new StateInsecurityException(state.StateId.ToString(), grainType, state.DoingVersion, state.Version);
            state.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<PrimaryKey>(this IObserverSnapshot<PrimaryKey> snapshot, EventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionUnorderedException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<PrimaryKey>(this IObserverSnapshot<PrimaryKey> snapshot, EventBase eventBase, Type grainType)
        {
            if (snapshot.Version > 0 && snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionUnorderedException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
    }
}
