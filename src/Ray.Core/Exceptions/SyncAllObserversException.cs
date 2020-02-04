using System;

namespace Ray.Core.Exceptions
{
    public class SyncAllObserversException : Exception
    {
        public SyncAllObserversException(string id, Type grainType) :
            base($"Sync all observers failed of Grain type {grainType.FullName} and Id {id}")
        {
        }
    }
}
