using System.Collections.Generic;
using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowActorMetricHandler
    {
        private readonly Gauge FollowActorMetricCountGauge;
        private readonly Gauge FollowActorMetricMaxElapsedMsGauge;
        private readonly Gauge FollowActorMetricAvgElapsedMsGauge;
        private readonly Gauge FollowActorMetricMinElapsedMsGauge;

        public FollowActorMetricHandler()
        {
            this.FollowActorMetricCountGauge = Metrics.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.Events)}", string.Empty, nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));
            this.FollowActorMetricMaxElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.MaxElapsedMs)}", string.Empty, nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));
            this.FollowActorMetricAvgElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.AvgElapsedMs)}", string.Empty, nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));
            this.FollowActorMetricMinElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.MinElapsedMs)}", string.Empty, nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));
        }

        public void Handle(List<FollowActorMetric> followActorMetrics)
        {
            foreach (var item in followActorMetrics)
            {
                this.FollowActorMetricCountGauge.WithLabels(item.Actor, item.FromActor).Set(item.Events, item.Timestamp);
                this.FollowActorMetricMaxElapsedMsGauge.WithLabels(item.Actor, item.FromActor).Set(item.MaxElapsedMs, item.Timestamp);
                this.FollowActorMetricAvgElapsedMsGauge.WithLabels(item.Actor, item.FromActor).Set(item.AvgElapsedMs, item.Timestamp);
                this.FollowActorMetricMinElapsedMsGauge.WithLabels(item.Actor, item.FromActor).Set(item.MinElapsedMs, item.Timestamp);
            }
        }
    }
}
