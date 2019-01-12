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
        public static void UpdateVersion<K, E>(this IActorState<K> state, IEvent<K, E> @event, Type grainType)
            where E : IEventBase<K>
        {
            if (state.Version + 1 != @event.Base.Version)
                throw new EventVersionNotMatchStateException(state.StateId.ToString(), grainType, @event.Base.Version, state.Version);
            state.Version = @event.Base.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<K, E>(this IActorState<K> state, IEvent<K, E> @event, Type grainType)
            where E : IEventBase<K>
        {
            if (state.Version + 1 != @event.Base.Version)
                throw new EventVersionNotMatchStateException(state.StateId.ToString(), grainType, @event.Base.Version, state.Version);
            state.DoingVersion = @event.Base.Version;
            state.Version = @event.Base.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeUpdateVersion<K>(this IActorState<K> state, long version, long timestamp)
        {
            state.DoingVersion = version;
            state.Version = version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<K>(this IActorState<K> state, Type grainType)
        {
            if (state.DoingVersion != state.Version)
                throw new StateInsecurityException(state.StateId.ToString(), grainType, state.DoingVersion, state.Version);
            state.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DecrementDoingVersion<K>(this IActorState<K> state)
        {
            state.DoingVersion -= 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetEventId<K, E>(this IEvent<K, E> @event)
            where E : IEventBase<K>
        {
            return $"{@event.Base.StateId.ToString()}_{@event.Base.Version.ToString()}";
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static EventUID GetNextUID<K, E>(this IEvent<K, E> @event)
            where E : IEventBase<K>
        {
            return new EventUID(@event.GetEventId(), @event.Base.Timestamp);
        }
    }
}
