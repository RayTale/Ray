using System;

namespace Ray.Core.Exceptions
{
    public class UnfindObserverUnitException : Exception
    {
        public UnfindObserverUnitException(string name) : base(name)
        {
        }
    }
}
