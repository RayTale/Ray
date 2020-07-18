using System;

namespace Ray.Core.Exceptions
{
    public class UnfindSnapshotHandlerException : Exception
    {
        public UnfindSnapshotHandlerException(Type grainType) : base(grainType.FullName)
        {

        }
    }
}
