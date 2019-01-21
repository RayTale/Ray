using System;

namespace Ray.Core.Exceptions
{
    public class UnMatchFollowUnitException : Exception
    {
        public UnMatchFollowUnitException(string grainName, string unitName) : base($"{unitName} and {grainName} do not match")
        {
        }
    }
}
