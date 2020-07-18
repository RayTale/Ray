using System.Collections.Generic;
using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowGroupMetricHandler
    {
        private readonly Gauge FollowGroupMetricCountGauge;
        private readonly Gauge FollowGroupMetricMaxElapsedMsGauge;
        private readonly Gauge FollowGroupMetricAvgElapsedMsGauge;
        private readonly Gauge FollowGroupMetricMinElapsedMsGauge;
        private readonly Gauge FollowGroupMetricMaxDeliveryElapsedMsGauge;
        private readonly Gauge FollowGroupMetricAvgDeliveryElapsedMsGauge;
        private readonly Gauge FollowGroupMetricMinDeliveryElapsedMsGauge;

        public FollowGroupMetricHandler()
        {
            this.FollowGroupMetricCountGauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.Events)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricMaxElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MaxElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricAvgElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.AvgElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricMinElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MinElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricMaxDeliveryElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MaxDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricAvgDeliveryElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.AvgDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricMinDeliveryElapsedMsGauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MinDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
        }

        public void Handle(List<FollowGroupMetric> followGroupMetrics)
        {
            foreach (var item in followGroupMetrics)
            {
                this.FollowGroupMetricCountGauge.WithLabels(item.Group).Set(item.Events, item.Timestamp);
                this.FollowGroupMetricMaxElapsedMsGauge.WithLabels(item.Group).Set(item.MaxElapsedMs, item.Timestamp);
                this.FollowGroupMetricAvgElapsedMsGauge.WithLabels(item.Group).Set(item.AvgElapsedMs, item.Timestamp);
                this.FollowGroupMetricMinElapsedMsGauge.WithLabels(item.Group).Set(item.MinElapsedMs, item.Timestamp);
                this.FollowGroupMetricMaxDeliveryElapsedMsGauge.WithLabels(item.Group).Set(item.MaxDeliveryElapsedMs, item.Timestamp);
                this.FollowGroupMetricAvgDeliveryElapsedMsGauge.WithLabels(item.Group).Set(item.AvgDeliveryElapsedMs, item.Timestamp);
                this.FollowGroupMetricMinDeliveryElapsedMsGauge.WithLabels(item.Group).Set(item.MinDeliveryElapsedMs, item.Timestamp);
            }
        }
    }
}
