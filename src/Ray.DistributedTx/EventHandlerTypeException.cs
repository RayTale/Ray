using System;

namespace Ray.DistributedTransaction
{
    public class EventHandlerTypeException : Exception
    {
        public EventHandlerTypeException(string message) : base(message)
        {
        }
    }
}
