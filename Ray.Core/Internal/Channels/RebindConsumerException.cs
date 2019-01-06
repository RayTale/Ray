using System;

namespace Ray.Core.Internal.Channels
{
    public class RebindConsumerException : Exception
    {
        public RebindConsumerException(string message) : base(message)
        {
        }
    }
}
