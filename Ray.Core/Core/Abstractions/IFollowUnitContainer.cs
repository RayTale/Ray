using System;

namespace Ray.Core.Abstractions
{
    public interface IFollowUnitContainer
    {
        IFollowUnit<PrimaryKey> GetUnit<PrimaryKey>(Type grainType);
        void Register(IGrainID followUnit);
    }
}
