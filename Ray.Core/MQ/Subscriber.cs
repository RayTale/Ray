using System;

namespace Ray.Core.MQ
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
