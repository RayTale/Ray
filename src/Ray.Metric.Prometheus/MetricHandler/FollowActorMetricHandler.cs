using System.Collections.Generic;
using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowActorMetricHandler
    {
        private readonly IMetricFamily<IGauge, (string Actor, string FromActor)> FollowActorMetricCountGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string FromActor)> FollowActorMetricMaxElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string FromActor)> FollowActorMetricAvgElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string FromActor)> FollowActorMetricMinElapsedMsGauge;

        public FollowActorMetricHandler(IMetricFactory metricFactory)
        {
            var labelNames = (nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));

            this.FollowActorMetricCountGauge = metricFactory.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.Events)}", string.Empty, labelNames);
            this.FollowActorMetricMaxElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.MaxElapsedMs)}", string.Empty, labelNames);
            this.FollowActorMetricAvgElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.AvgElapsedMs)}", string.Empty, labelNames);
            this.FollowActorMetricMinElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.MinElapsedMs)}", string.Empty, labelNames);
        }

        public void Handle(List<FollowActorMetric> followActorMetrics)
        {
            foreach (var item in followActorMetrics)
            {
                var labels = (item.Actor, item.FromActor);
                
                this.FollowActorMetricCountGauge.WithLabels(labels).Set(item.Events, item.Timestamp);
                this.FollowActorMetricMaxElapsedMsGauge.WithLabels(labels).Set(item.MaxElapsedMs, item.Timestamp);
                this.FollowActorMetricAvgElapsedMsGauge.WithLabels(labels).Set(item.AvgElapsedMs, item.Timestamp);
                this.FollowActorMetricMinElapsedMsGauge.WithLabels(labels).Set(item.MinElapsedMs, item.Timestamp);
            }
        }
    }
}
