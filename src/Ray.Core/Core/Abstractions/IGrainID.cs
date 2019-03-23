using System;

namespace Ray.Core.Abstractions
{
    public interface IGrainID
    {
        Type GrainType { get; }
    }
}
