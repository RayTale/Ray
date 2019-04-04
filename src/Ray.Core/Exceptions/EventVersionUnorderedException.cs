using System;

namespace Ray.Core.Exceptions
{
    public class EventVersionUnorderedException : Exception
    {
        public EventVersionUnorderedException(string id, Type type, long eventVersion, long stateVersion) :
            base($"Event version and state version do not match of Grain type {type.FullName} and Id {id}.There state version are {stateVersion} and event version are {eventVersion}")
        {
        }
    }
}
