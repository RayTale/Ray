using System;
using System.Collections.Concurrent;
using Ray.Core.Abstractions;
using Ray.Core.Exceptions;

namespace Ray.Core
{
    public class FollowUnitContainer : IFollowUnitContainer
    {
        readonly ConcurrentDictionary<Type, object> unitDict = new ConcurrentDictionary<Type, object>();
        public IFollowUnit<K> GetUnit<K>(Type grainType)
        {
            if (unitDict.TryGetValue(grainType, out var unit))
            {
                if (unit is IFollowUnit<K> result)
                {
                    return result;
                }
                else
                    throw new UnMatchFollowUnitException(grainType.FullName, unit.GetType().FullName);
            }
            else
                throw new UnfindFollowUnitException(grainType.FullName);
        }

        public void Register(IGrainID followUnit)
        {
            if (!unitDict.TryAdd(followUnit.GrainType, followUnit))
            {
                throw new RepeatedFollowUnitException(followUnit.GrainType.FullName);
            }
        }
    }
}
