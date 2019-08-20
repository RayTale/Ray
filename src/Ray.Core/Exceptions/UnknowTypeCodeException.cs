using System;

namespace Ray.Core.Exceptions
{
    public class UnknowTypeCodeException : Exception
    {
        public UnknowTypeCodeException(string typeName) : base(typeName)
        {
        }
    }
}
