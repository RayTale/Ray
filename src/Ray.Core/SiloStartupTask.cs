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
        private readonly IServiceProvider serviceProvider;
        private readonly IStartupConfig startupConfig;

        public SiloStartupTask(IServiceProvider serviceProvider, IStartupConfig startupConfig)
        {
            this.serviceProvider = serviceProvider;
            this.startupConfig = startupConfig;
        }

        public async Task Execute(CancellationToken cancellationToken)
        {
            await this.startupConfig.ConfigureObserverUnit(this.serviceProvider, this.serviceProvider.GetService<IObserverUnitContainer>());
            await Startup.StartRay(this.serviceProvider);
        }
    }
}
