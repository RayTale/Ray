﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Ray.Core.Abstractions.Observer;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Observer;
using Ray.Core.Snapshot;
using Ray.Core.Utils;

namespace Ray.Core
{
    public static class CoreExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<PrimaryKey>(this SnapshotBase<PrimaryKey> snapshot, EventBasicInfo eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
            {
                throw new EventVersionUnorderedException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            }

            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
            {
                snapshot.StartTimestamp = eventBase.Timestamp;
            }

            if (eventBase.Timestamp < snapshot.LatestMinEventTimestamp)
            {
                snapshot.LatestMinEventTimestamp = eventBase.Timestamp;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<PrimaryKey>(this SnapshotBase<PrimaryKey> snapshot, EventBasicInfo eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
            {
                throw new EventVersionUnorderedException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            }

            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
            {
                snapshot.StartTimestamp = eventBase.Timestamp;
            }

            if (eventBase.Timestamp < snapshot.LatestMinEventTimestamp)
            {
                snapshot.LatestMinEventTimestamp = eventBase.Timestamp;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<PrimaryKey>(this SnapshotBase<PrimaryKey> snapshot, Type grainType)
        {
            if (snapshot.DoingVersion != snapshot.Version)
            {
                throw new StateInsecurityException(snapshot.StateId.ToString(), grainType, snapshot.DoingVersion, snapshot.Version);
            }

            snapshot.DoingVersion++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DecrementDoingVersion<PrimaryKey>(this SnapshotBase<PrimaryKey> snapshot) => snapshot.DoingVersion--;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetEventId(this EventBasicInfo eventBase, string stateId)
        {
            return $"{stateId}_{eventBase.Version}";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetEventId<PrimaryKey>(this FullyEvent<PrimaryKey> @event)
        {
            return $"{@event.StateId}_{@event.BasicInfo.Version}";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static EventUID GetNextUID<PrimaryKey>(this FullyEvent<PrimaryKey> @event, string fromActor)
        {
            return new EventUID(@event.GetEventId(), @event.BasicInfo.Timestamp, @event.Event.GetType().Name, fromActor, @event.StateId.ToString(), @event.BasicInfo.Version);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeUpdateVersion<PrimaryKey>(this IObserverSnapshot<PrimaryKey> snapshot, EventBasicInfo eventBase)
        {
            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
            {
                snapshot.StartTimestamp = eventBase.Timestamp;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<PrimaryKey>(this IObserverSnapshot<PrimaryKey> state, Type grainType)
        {
            if (state.DoingVersion != state.Version)
            {
                throw new StateInsecurityException(state.StateId.ToString(), grainType, state.DoingVersion, state.Version);
            }

            state.DoingVersion++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateVersion<PrimaryKey>(this IObserverSnapshot<PrimaryKey> snapshot, EventBasicInfo eventBase, Type grainType)
        {
            if (snapshot.Version + 1 != eventBase.Version)
            {
                throw new EventVersionUnorderedException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            }

            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
            {
                snapshot.StartTimestamp = eventBase.Timestamp;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<PrimaryKey>(this IObserverSnapshot<PrimaryKey> snapshot, EventBasicInfo eventBase, Type grainType)
        {
            if (snapshot.Version > 0 && snapshot.Version + 1 != eventBase.Version)
            {
                throw new EventVersionUnorderedException(snapshot.StateId.ToString(), grainType, eventBase.Version, snapshot.Version);
            }

            snapshot.DoingVersion = eventBase.Version;
            snapshot.Version = eventBase.Version;
            if (snapshot.StartTimestamp == 0 || eventBase.Timestamp < snapshot.StartTimestamp)
            {
                snapshot.StartTimestamp = eventBase.Timestamp;
            }
        }
        private static List<(Type type, ObserverAttribute observer)> observerAttributeList;

        /// <summary>
        /// Gets the types of all marked grains from the cache
        /// </summary>
        public static List<(Type type, ObserverAttribute observer)> AllObserverAttribute
        {
            get
            {
                if (observerAttributeList is null)
                {
                    observerAttributeList = new List<(Type type, ObserverAttribute observer)>();
                    foreach (var assembly in AssemblyHelper.GetAssemblies())
                    {
                        foreach (var type in assembly.GetTypes().Where(t => typeof(IObserver).IsAssignableFrom(t)))
                        {
                            foreach (var attribute in type.GetCustomAttributes(false))
                            {
                                if (attribute is ObserverAttribute observer)
                                {
                                    observerAttributeList.Add((type, observer));
                                }
                            }
                        }
                    }
                }
                return observerAttributeList;
            }
        }
    }
}
