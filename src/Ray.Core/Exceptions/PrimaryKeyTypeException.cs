using System;

namespace Ray.Core.Exceptions
{
    public class PrimaryKeyTypeException : Exception
    {
        public PrimaryKeyTypeException(string name) : base(name)
        {

        }
    }
}
