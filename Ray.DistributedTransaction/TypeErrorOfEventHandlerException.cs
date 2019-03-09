using System;

namespace Ray.DistributedTransaction
{
    public class TypeErrorOfEventHandlerException : Exception
    {
        public TypeErrorOfEventHandlerException(string message) : base(message)
        {
        }
    }
}
