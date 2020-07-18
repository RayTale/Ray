using System;

namespace Ray.Core.Exceptions
{
    public class ObserverNotCompletedException : Exception
    {
        public ObserverNotCompletedException(string typeName, string stateId) : base($"{typeName} with id={stateId}")
        {

        }
    }
} 
