using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions.Monitor;
using Ray.DistributedTx.Abstractions;

namespace Ray.Metrics
{
    public static class Extensions
    {
        public static void AddRayMetric(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<MetricMonitor>();
            serviceCollection.AddSingleton<IMetricMonitor>(provider => provider.GetService<MetricMonitor>());
            serviceCollection.AddSingleton<IDTxMetricMonitor>(provider => provider.GetService<MetricMonitor>());
        }
    }
}
