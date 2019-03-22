using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Abstractions;
using RayTest.Grains;

namespace RayCore.Tests
{
    public class Configuration : IStartupConfig
    {
        public void Configure(IServiceCollection serviceCollection)
        {

        }

        public Task ConfigureFollowUnit(IServiceProvider serviceProvider, IObserverUnitContainer followUnitContainer)
        {
            followUnitContainer.Register(ObserverUnit<long>.From<Account>(serviceProvider));
            return Task.CompletedTask;
        }
    }
}
