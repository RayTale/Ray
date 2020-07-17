using System;

namespace Ray.Core.Exceptions
{
    public class UnknownTypeCodeException : Exception
    {
        public UnknownTypeCodeException(string typeName) : base(typeName)
        {
        }
    }
}
