using System;

namespace Ray.Core.Exceptions
{
    public class UnmatchObserverUnitException : Exception
    {
        public UnmatchObserverUnitException(string grainName, string unitName) : base($"{unitName} and {grainName} do not match")
        {
        }
    }
}
