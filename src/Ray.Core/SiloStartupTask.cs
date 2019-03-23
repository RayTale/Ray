using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Ray.Core.Abstractions;

namespace Ray.Core
{
    public class SiloStartupTask : IStartupTask
    {
        readonly IServiceProvider serviceProvider;
        readonly IStartupConfig startupConfig;
        public SiloStartupTask(IServiceProvider serviceProvider, IStartupConfig startupConfig)
        {
            this.serviceProvider = serviceProvider;
            this.startupConfig = startupConfig;
        }
        public async Task Execute(CancellationToken cancellationToken)
        {
            await startupConfig.ConfigureObserverUnit(serviceProvider, serviceProvider.GetService<IObserverUnitContainer>());
            await Startup.StartRay(serviceProvider);
        }
    }
}
