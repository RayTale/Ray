using System;

namespace Ray.EventBus.Kafka
{
    public class EventBusRepeatException : Exception
    {
        public EventBusRepeatException(string message) : base(message)
        {
        }
    }
}
