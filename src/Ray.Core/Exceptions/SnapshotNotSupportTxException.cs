using System;

namespace Ray.Core.Exceptions
{
    public class SnapshotNotSupportTxException : Exception
    {
        public SnapshotNotSupportTxException(Type type) : base(type.FullName)
        {
        }
    }
}
