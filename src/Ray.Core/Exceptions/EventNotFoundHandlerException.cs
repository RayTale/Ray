using System;

namespace Ray.Core.Exceptions
{
    public class EventNotFoundHandlerException : Exception
    {
        public EventNotFoundHandlerException(Type eventType) : base($"not found {eventType.FullName}'s handler")
        {

        }
    }
}
