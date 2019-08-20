using System;

namespace Ray.Core.Exceptions
{
    public class UnfindEventHandlerException : Exception
    {
        public UnfindEventHandlerException(Type eventType) : base($"not found {eventType.FullName}'s handler")
        {

        }
    }
}
