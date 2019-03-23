using System;

namespace Ray.Core.Exceptions
{
    public class ObserverUnitRepeatedException : Exception
    {
        public ObserverUnitRepeatedException(string name) : base(name)
        {
        }
    }
}
