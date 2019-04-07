using System;

namespace Ray.Core.Exceptions
{
    public class StateIsOverException : Exception
    {
        public StateIsOverException(string id, Type grainType) :
           base($"State Is Over of Grain type {grainType.FullName} and Id {id}")
        {
        }
    }
}
