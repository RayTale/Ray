using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Abstractions;
using RayTest.Grains;
using System;
using System.Threading.Tasks;

namespace RayCore.Tests
{
    public class Configuration : IStartupConfig
    {
        public void Configure(IServiceCollection serviceCollection)
        {
        }

        public Task ConfigureObserverUnit(IServiceProvider serviceProvider, IObserverUnitContainer followUnitContainer)
        {
            followUnitContainer.Register(ObserverUnit<long>.From<Account>(serviceProvider));
            return Task.CompletedTask;
        }
    }
}
