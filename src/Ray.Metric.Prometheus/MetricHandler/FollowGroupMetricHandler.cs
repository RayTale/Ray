using System;
using System.Collections.Generic;
using Prometheus.Client;
using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowGroupMetricHandler
    {
        private readonly IMetricFamily<IGauge, ValueTuple<string>> FollowGroupMetricCountGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> FollowGroupMetricMaxElapsedMsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> FollowGroupMetricAvgElapsedMsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> FollowGroupMetricMinElapsedMsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> FollowGroupMetricMaxDeliveryElapsedMsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> FollowGroupMetricAvgDeliveryElapsedMsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> FollowGroupMetricMinDeliveryElapsedMsGauge;

        public FollowGroupMetricHandler(IMetricFactory metricFactory)
        {
            this.FollowGroupMetricCountGauge = metricFactory.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.Events)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricMaxElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MaxElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricAvgElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.AvgElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricMinElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MinElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricMaxDeliveryElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MaxDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricAvgDeliveryElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.AvgDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            this.FollowGroupMetricMinDeliveryElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MinDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
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
