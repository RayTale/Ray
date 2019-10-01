using System;

namespace Ray.Core.Channels
{
    public class NoBindConsumerException : Exception
    {
        public NoBindConsumerException(string message) : base(message)
        {
        }
    }
}
