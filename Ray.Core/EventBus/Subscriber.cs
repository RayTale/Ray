using System;

namespace Ray.Core.EventBus
{
    public class Subscriber
    {
        public Subscriber(Type handler)
        {
            Handler = handler;
        }
        public Type Handler { get; }
    }
}
