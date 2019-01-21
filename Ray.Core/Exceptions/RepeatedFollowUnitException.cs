using System;

namespace Ray.Core.Exceptions
{
    public class RepeatedFollowUnitException : Exception
    {
        public RepeatedFollowUnitException(string name) : base(name)
        {
        }
    }
}
