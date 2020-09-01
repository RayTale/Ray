using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Prometheus.Client.DependencyInjection;
using Ray.Metric.Core;
using Ray.Metric.Prometheus.MetricHandler;

namespace Ray.Metric.Prometheus
{
    public static class Extensions
    {
        public static ISiloBuilder MetricPushToPrometheus(
          this ISiloBuilder builder,
          Action<MetricOption> configAction)
        {
            builder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddMetricFactory();
                serviceCollection.Configure<MetricOption>(config => configAction(config));
                serviceCollection.AddHostedService<MetricPushService>();
                serviceCollection.AddSingleton<IMetricStream, PrometheusMetricStream>();
                serviceCollection.AddSingleton<EventMetricHandler>();
                serviceCollection.AddSingleton<ActorMetricHandler>();
                serviceCollection.AddSingleton<EventSummaryMetricHandler>();
                serviceCollection.AddSingleton<EventLinkMetricHandler>();
                serviceCollection.AddSingleton<FollowActorMetricHandler>();
                serviceCollection.AddSingleton<FollowFollowEventMetricHandler>();
                serviceCollection.AddSingleton<FollowGroupMetricHandler>();
                serviceCollection.AddSingleton<SnapshotMetricHandler>();
                serviceCollection.AddSingleton<SnapshotSummaryMetricHandler>();
                serviceCollection.AddSingleton<DtxMetricHandler>();
                serviceCollection.AddSingleton<DtxSummaryMetricHandler>();
            });
            return builder;
        }
    }
}
