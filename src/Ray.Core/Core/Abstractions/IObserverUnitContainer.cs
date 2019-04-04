using System;

namespace Ray.Core.Abstractions
{
    public interface IObserverUnitContainer
    {
        IObserverUnit<PrimaryKey> GetUnit<PrimaryKey>(Type grainType);
        object GetUnit(Type grainType);
        void Register(IGrainID followUnit);
    }
}
