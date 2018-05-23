using System;

namespace Ray.Core.Exceptions
{
    public class UnknowTypeCodeException : Exception
    {
        public UnknowTypeCodeException(string typeCode) : base($"The {typeCode} corresponding type was not found")
        {
        }
    }
}
