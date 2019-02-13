using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Core.Abstractions
{
    public interface IStartupConfig
    {
        Task ConfigureFollowUnit(IServiceProvider serviceProvider, IFollowUnitContainer followUnitContainer);
        void Configure(IServiceCollection serviceCollection);
    }
}
