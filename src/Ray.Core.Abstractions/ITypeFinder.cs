using System;

namespace Ray.Core.Abstractions
{
    public interface ITypeFinder
    {
        Type FindType(string code);
        string GetCode(Type type);
    }
}
