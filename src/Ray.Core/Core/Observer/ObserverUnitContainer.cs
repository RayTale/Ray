using System;
using System.Collections.Concurrent;
using Ray.Core.Abstractions;
using Ray.Core.Exceptions;

namespace Ray.Core
{
    public class ObserverUnitContainer : IObserverUnitContainer
    {
        readonly ConcurrentDictionary<Type, object> unitDict = new ConcurrentDictionary<Type, object>();
        public IObserverUnit<PrimaryKey> GetUnit<PrimaryKey>(Type grainType)
        {
            if (unitDict.TryGetValue(grainType, out var unit))
            {
                if (unit is IObserverUnit<PrimaryKey> result)
                {
                    return result;
                }
                else
                    throw new UnMatchObserverUnitException(grainType.FullName, unit.GetType().FullName);
            }
            else
                throw new UnfindObserverUnitException(grainType.FullName);
        }

        public void Register(IGrainID followUnit)
        {
            if (!unitDict.TryAdd(followUnit.GrainType, followUnit))
            {
                throw new ObserverUnitRepeatedException(followUnit.GrainType.FullName);
            }
        }
    }
}
