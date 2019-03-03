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
        public static void Apply<PrimaryKey, SnapshotType>(this Snapshot<PrimaryKey, SnapshotType> snapshot, IEventHandler<PrimaryKey, SnapshotType> handler, IFullyEvent<PrimaryKey> fullyEvent)
             where SnapshotType : class, new()
        {
            switch (fullyEvent.Event)
            {
                case TransactionFinishEvent _:
                    {
                        snapshot.Base.ClearTransactionInfo(false);
                    }; break;
                case TransactionCommitEvent transactionCommitEvent:
                    {
                        snapshot.Base.TransactionStartVersion = transactionCommitEvent.StartVersion;
                        snapshot.Base.TransactionStartTimestamp = transactionCommitEvent.StartTimestamp;
                        snapshot.Base.TransactionId = transactionCommitEvent.Id;
                    }; break;
                default: handler.Apply(snapshot, fullyEvent); break;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<PrimaryKey>(this ISnapshotBase<PrimaryKey> snapshot, IEventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
            if (eventBase.Timestamp < snapshot.LatestMinEventTimestamp)
                snapshot.LatestMinEventTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<PrimaryKey>(this ISnapshotBase<PrimaryKey> snapshot, IEventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
            if (eventBase.Timestamp < snapshot.LatestMinEventTimestamp)
                snapshot.LatestMinEventTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<K>(this ISnapshotBase<K> snapshot, Type grainType)
        {
            if (snapshot.DoingVersion != snapshot.Version)
                throw new StateInsecurityException(snapshot.StateId.ToString(), grainType, snapshot.DoingVersion, snapshot.Version);
            snapshot.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DecrementDoingVersion<K>(this ISnapshotBase<K> snapshot)
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
        public static void UnsafeUpdateVersion<PrimaryKey>(this IFollowSnapshot<PrimaryKey> snapshot, IEventBase eventBase)
        {
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
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
        public static void UpdateVersion<PrimaryKey>(this IFollowSnapshot<PrimaryKey> snapshot, IEventBase eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<PrimaryKey>(this IFollowSnapshot<PrimaryKey> snapshot, IEventBase eventBase, Type grainType)
        {
            if (snapshot.Version > 0 && snapshot.Version + 1 != eventBase.Version)
                throw new EventVersionNotMatchStateException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
                snapshot.StartTimestamp = eventBase.Timestamp;
        }
    }
}
