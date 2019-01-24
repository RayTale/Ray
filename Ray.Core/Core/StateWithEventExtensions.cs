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
        public static void UpdateVersion<K, B, E>(this IState<K, B> state, IEvent<K, E> @event, Type grainType)
            where E : IEventBase<K>
            where B : ISnapshot<K>, new()
        {
            if (state.Base.Version + 1 != @event.Base.Version)
                throw new EventVersionNotMatchStateException(state.Base.StateId.ToString(), grainType, @event.Base.Version, state.Base.Version);
            state.Base.Version = @event.Base.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FullUpdateVersion<K, B, E>(this IState<K, B> state, IEvent<K, E> @event, Type grainType)
            where E : IEventBase<K>
            where B : ISnapshot<K>, new()
        {
            if (state.Base.Version + 1 != @event.Base.Version)
                throw new EventVersionNotMatchStateException(state.Base.StateId.ToString(), grainType, @event.Base.Version, state.Base.Version);
            state.Base.DoingVersion = @event.Base.Version;
            state.Base.Version = @event.Base.Version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeUpdateVersion<K, B>(this IState<K, B> state, long version, long timestamp)
            where B : ISnapshot<K>, new()
        {
            state.Base.DoingVersion = version;
            state.Base.Version = version;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void IncrementDoingVersion<K, B>(this IState<K, B> state, Type grainType)
            where B : ISnapshot<K>, new()
        {
            if (state.Base.DoingVersion != state.Base.Version)
                throw new StateInsecurityException(state.Base.StateId.ToString(), grainType, state.Base.DoingVersion, state.Base.Version);
            state.Base.DoingVersion += 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DecrementDoingVersion<K, B>(this IState<K, B> state)
            where B : ISnapshot<K>, new()
        {
            state.Base.DoingVersion -= 1;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetEventId<K, E>(this IEvent<K, E> @event)
            where E : IEventBase<K>, new()
        {
            return $"{@event.Base.StateId.ToString()}_{@event.Base.Version.ToString()}";
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static EventUID GetNextUID<K, E>(this IEvent<K, E> @event)
            where E : IEventBase<K>, new()
        {
            return new EventUID(@event.GetEventId(), @event.Base.Timestamp);
        }
    }
}
