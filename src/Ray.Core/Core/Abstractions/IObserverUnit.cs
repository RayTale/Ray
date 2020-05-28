using System;

namespace Ray.Core.Abstractions
{
    public interface IObserverUnit
    {
        Type GrainType { get; }
    }
}
