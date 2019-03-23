using System;

namespace Ray.Core.Channels
{
    public class RebindConsumerException : Exception
    {
        public RebindConsumerException(string message) : base(message)
        {
        }
    }
}
