using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Ray.Core.Abstractions.Monitor;
using Ray.DistributedTx.Abstractions;
using Ray.Metric.Core.Options;
using System;

namespace Ray.Metric.Core
{
    public static class Extensions
    {
        public static ISiloBuilder AddRayMetric(this ISiloBuilder builder, Action<MonitorOptions> optionInit = null)
        {
            builder.ConfigureServices(service =>
            {
                service.AddSingleton<MetricMonitor>();
                service.AddSingleton<IMetricMonitor>(provider => provider.GetService<MetricMonitor>());
                service.AddSingleton<IDTxMetricMonitor>(provider => provider.GetService<MetricMonitor>());
            });
            if (optionInit != default)
            {
                builder.Configure(optionInit);
            }
            builder.AddSimpleMessageStreamProvider("MetricProvider").AddMemoryGrainStorage("PubSubStore");
            return builder;
        }
    }
}
