using System;

namespace Ray.DistributedTransaction
{
    public class SnapshotHandlerTypeException : Exception
    {
        public SnapshotHandlerTypeException(string message) : base(message)
        {
        }
    }
}
