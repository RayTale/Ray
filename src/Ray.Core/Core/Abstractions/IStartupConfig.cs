using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.Abstractions
{
    public interface IStartupConfig
    {
        Task ConfigureObserverUnit(IServiceProvider serviceProvider, IObserverUnitContainer followUnitContainer);
        void Configure(IServiceCollection serviceCollection);
    }
}
