using System.Collections.Generic;
using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowFollowEventMetricHandler
    {
        private readonly Gauge FollowEventMetricCountGauge;
        private readonly Gauge FollowEventMetricMaxElapsedMsGauge;
        private readonly Gauge FollowEventMetricAvgElapsedMsGauge;
        private readonly Gauge FollowEventMetricMinElapsedMsGauge;
        private readonly Gauge FollowEventMetricMaxDeliveryElapsedMsGauge;
        private readonly Gauge FollowEventMetricAvgDeliveryElapsedMsGauge;
        private readonly Gauge FollowEventMetricMinDeliveryElapsedMsGauge;

        public FollowFollowEventMetricHandler()
        {
            this.FollowEventMetricCountGauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.Events)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            this.FollowEventMetricMaxElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MaxElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            this.FollowEventMetricAvgElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.AvgElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            this.FollowEventMetricMinElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MinElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            this.FollowEventMetricMaxDeliveryElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MaxDeliveryElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            this.FollowEventMetricAvgDeliveryElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.AvgDeliveryElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            this.FollowEventMetricMinDeliveryElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MinDeliveryElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
        }

        public void Handle(List<FollowEventMetric> followEventMetrics)
        {
            foreach (var item in followEventMetrics)
            {
                this.FollowEventMetricCountGauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.Events, item.Timestamp);
                this.FollowEventMetricMaxElapsedMsGauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.MaxElapsedMs, item.Timestamp);
                this.FollowEventMetricAvgElapsedMsGauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.AvgElapsedMs, item.Timestamp);
                this.FollowEventMetricMinElapsedMsGauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.MinElapsedMs, item.Timestamp);
                this.FollowEventMetricMaxDeliveryElapsedMsGauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.MaxDeliveryElapsedMs, item.Timestamp);
                this.FollowEventMetricAvgDeliveryElapsedMsGauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.AvgDeliveryElapsedMs, item.Timestamp);
                this.FollowEventMetricMinDeliveryElapsedMsGauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.MinDeliveryElapsedMs, item.Timestamp);
            }
        }
    }
}
