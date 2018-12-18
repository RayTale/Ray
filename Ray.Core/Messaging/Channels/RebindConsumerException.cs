using System;

namespace Ray.Core.Messaging.Channels
{
    public class RebindConsumerException : Exception
    {
        public RebindConsumerException(string message) : base(message)
        {
        }
    }
}
