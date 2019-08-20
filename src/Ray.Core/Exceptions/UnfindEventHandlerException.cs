using System;

namespace Ray.Core.Exceptions
{
    public class UnfindEventHandlerException : Exception
    {
        public UnfindEventHandlerException(Type eventType) : base(eventType.FullName)
        {

        }
    }
}
