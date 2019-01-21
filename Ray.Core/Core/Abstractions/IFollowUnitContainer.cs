using System;

namespace Ray.Core.Abstractions
{
    public interface IFollowUnitContainer
    {
        IFollowUnit<K> GetUnit<K>(Type grainType);
        void Register(IGrainID followUnit);
    }
}
