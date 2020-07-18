using System;

namespace Ray.Core.Exceptions
{
    public class StateInsecurityException : Exception
    {
        public StateInsecurityException(string id, Type grainType, long doingVersion, long stateVersion) :
            base($"State insecurity of Grain type {grainType.FullName} and Id {id},Maybe because the previous event failed to execute.There state version are {stateVersion} and doing version are {doingVersion}")
        {
        }
    }
}
