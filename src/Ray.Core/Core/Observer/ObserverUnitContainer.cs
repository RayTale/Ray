using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Abstractions;
using Ray.Core.Exceptions;
using Ray.Core.Observer;
using Ray.Core.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Ray.Core
{
    public class ObserverUnitContainer : IObserverUnitContainer
    {
        readonly ConcurrentDictionary<Type, object> unitDict = new ConcurrentDictionary<Type, object>();
        public ObserverUnitContainer(IServiceProvider serviceProvider)
        {
            var observableList = new List<Type>();
            var observerList = new List<ObserverAttribute>();
            foreach (var assembly in AssemblyHelper.GetAssemblies(serviceProvider.GetService<ILogger<ObserverUnitContainer>>()))
            {
                foreach (var type in assembly.GetTypes())
                {
                    if (type != typeof(IObservable) &&
                        typeof(IObservable).IsAssignableFrom(type) &&
                        !type.IsGenericType)
                    {
                        observableList.Add(type);
                    }
                    else
                    {
                        foreach (var attribute in type.GetCustomAttributes(false))
                        {
                            if (attribute is ObserverAttribute observer)
                            {
                                if (observer.Observer is null)
                                {
                                    observer.Observer = type.GetInterfaces().SingleOrDefault(t =>
                                    (typeof(IGrainWithStringKey).IsAssignableFrom(t) || typeof(IGrainWithIntegerKey).IsAssignableFrom(t)) &&
                                    t != typeof(IGrainWithStringKey) &&
                                    t != typeof(IGrainWithIntegerKey));
                                }
                                if (observer.Observer is null)
                                    throw new NullReferenceException($"{nameof(ObserverAttribute.Observer)} in {type.FullName}");
                                observerList.Add(observer);
                                break;
                            }
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
                    throw new PrimaryKeyTypeException($"Ray can't support {observable.FullName} primary key type.");
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
                    throw new UnmatchObserverUnitException(grainType.FullName, unit.GetType().FullName);
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

        public void Register(IGrainID observerUnit)
        {
            if (!unitDict.TryAdd(observerUnit.GrainType, observerUnit))
            {
                throw new ObserverUnitRepeatedException(observerUnit.GrainType.FullName);
            }
        }
    }
}
