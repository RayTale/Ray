using System;

namespace Ray.Core.Exceptions
{
    public class UnknowTypeNameException : Exception
    {
        public UnknowTypeNameException(string typeName) : base($"Type named {typeName} was not found.")
        {
        }
    }
}
