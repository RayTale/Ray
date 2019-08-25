using System;

namespace Ray.DistributedTx
{
    public class SnapshotHandlerTypeException : Exception
    {
        public SnapshotHandlerTypeException(string message) : base(message)
        {
        }
    }
}
