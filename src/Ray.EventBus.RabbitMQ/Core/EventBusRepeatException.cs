using System;

namespace Ray.EventBus.RabbitMQ
{
    public class EventBusRepeatException : Exception
    {
        public EventBusRepeatException(string message) : base(message)
        {
        }
    }
}
