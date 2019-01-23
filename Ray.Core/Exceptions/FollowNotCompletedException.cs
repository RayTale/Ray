using System;

namespace Ray.Core.Exceptions
{
    public class FollowNotCompletedException : Exception
    {
        public FollowNotCompletedException(string typeName, string stateId) : base($"{typeName} with id={stateId}")
        {

        }
    }
} 
