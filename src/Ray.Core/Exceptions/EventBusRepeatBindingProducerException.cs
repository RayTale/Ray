using System;

namespace Ray.Core.Exceptions
{
    public class EventBusRepeatBindingProducerException : Exception
    {
        public EventBusRepeatBindingProducerException(string name) : base(name)
        {
        }
    }
}
