using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Ray.Core.Abstractions;
using Ray.Core.Exceptions;
using Ray.Core.Core.Observer;
using Orleans;

namespace Ray.Core
{
    public class ObserverUnitContainer : IObserverUnitContainer
    {
        readonly ConcurrentDictionary<Type, object> unitDict = new ConcurrentDictionary<Type, object>();
        public ObserverUnitContainer(IServiceProvider serviceProvider)
        {
            var observableList = new List<Type>();
            var observerList = new List<ObserverAttribute>();
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes())
                {
                    foreach (var attribute in type.GetCustomAttributes(false))
                    {
                        if (attribute is ObservableAttribute observable)
                        {
                            observableList.Add(type);
                            break;
                        }
                        if (attribute is ObserverAttribute observer)
                        {
                            observerList.Add(observer);
                            break;
                        }
                    }
                }
            }
            foreach (var observable in observableList)
            {
                if (typeof(IGrainWithIntegerKey).IsAssignableFrom(observable))
                {
                    var unitType = typeof(ObserverUnit<>).MakeGenericType(new Type[] { typeof(long) });
                    var unit = (ObserverUnit<long>)Activator.CreateInstance(unitType, serviceProvider, observable);
                    foreach (var observer in observerList.Where(o => o.Observable == observable))
                    {
                        unit.Observer(observer.Group, observer.Observer);
                    }
                    Register(unit);
                }
                else if (typeof(IGrainWithStringKey).IsAssignableFrom(observable))
                {
                    var unitType = typeof(ObserverUnit<>).MakeGenericType(new Type[] { typeof(string) });
                    var unit = (ObserverUnit<string>)Activator.CreateInstance(unitType, serviceProvider, observable);
                    foreach (var observer in observerList.Where(o => o.Observable == observable))
                    {
                        unit.Observer(observer.Group, observer.Observer);
                    }
                    Register(unit);
                }
                else
                    throw new PrimaryKeyTypeException(observable.FullName);
            }
        }
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
        public object GetUnit(Type grainType)
        {
            if (unitDict.TryGetValue(grainType, out var unit))
            {
                return unit;
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
