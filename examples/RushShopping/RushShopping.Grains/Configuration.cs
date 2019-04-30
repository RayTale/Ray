using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;

namespace RushShopping.Grains
{
    public class Configuration : IStartupConfig
    {
        #region Implementation of IStartupConfig

        public Task ConfigureObserverUnit(IServiceProvider serviceProvider, IObserverUnitContainer followUnitContainer)
        {
            return Task.CompletedTask;
        }

        public void Configure(IServiceCollection serviceCollection)
        {

        }

        #endregion
    }
}