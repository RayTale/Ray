using System;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Ray.Core
{
    public class SiloStartupTask : IStartupTask
    {
        readonly IServiceProvider serviceProvider;
        public SiloStartupTask(IServiceProvider serviceProvider) => this.serviceProvider = serviceProvider;
        public Task Execute(CancellationToken cancellationToken)
        {
            return Startup.StartRay(serviceProvider);
        }
    }
}
