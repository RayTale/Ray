using System;

namespace Ray.Core.Exceptions
{
    public class EventBusMultiplebindingProducerException : Exception
    {
        public EventBusMultiplebindingProducerException(string name) : base(name)
        {
        }
    }
}
