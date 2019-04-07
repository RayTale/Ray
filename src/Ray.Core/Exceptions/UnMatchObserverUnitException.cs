using System;

namespace Ray.Core.Exceptions
{
    public class UnMatchObserverUnitException : Exception
    {
        public UnMatchObserverUnitException(string grainName, string unitName) : base($"{unitName} and {grainName} do not match")
        {
        }
    }
}
