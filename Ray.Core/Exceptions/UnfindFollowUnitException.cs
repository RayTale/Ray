using System;

namespace Ray.Core.Exceptions
{
    public class UnfindFollowUnitException : Exception
    {
        public UnfindFollowUnitException(string name) : base(name)
        {
        }
    }
}
