using Prometheus.Client;
using Ray.Metric.Core.Element;
using System.Collections.Generic;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowGroupMetricHandler
    {
        readonly Gauge _FollowGroupMetric_Count_Gauge;
        readonly Gauge _FollowGroupMetric_MaxElapsedMs_Gauge;
        readonly Gauge _FollowGroupMetric_AvgElapsedMs_Gauge;
        readonly Gauge _FollowGroupMetric_MinElapsedMs_Gauge;
        readonly Gauge _FollowGroupMetric_MaxDeliveryElapsedMs_Gauge;
        readonly Gauge _FollowGroupMetric_AvgDeliveryElapsedMs_Gauge;
        readonly Gauge _FollowGroupMetric_MinDeliveryElapsedMs_Gauge;
        public FollowGroupMetricHandler()
        {
            _FollowGroupMetric_Count_Gauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.Events)}", string.Empty, nameof(FollowGroupMetric.Group));
            _FollowGroupMetric_MaxElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MaxElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            _FollowGroupMetric_AvgElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.AvgElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            _FollowGroupMetric_MinElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MinElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            _FollowGroupMetric_MaxDeliveryElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MaxDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            _FollowGroupMetric_AvgDeliveryElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.AvgDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
            _FollowGroupMetric_MinDeliveryElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowGroupMetric)}_{nameof(FollowGroupMetric.MinDeliveryElapsedMs)}", string.Empty, nameof(FollowGroupMetric.Group));
        }
        public void Handle(List<FollowGroupMetric> followGroupMetrics)
        {
            foreach (var item in followGroupMetrics)
            {
                _FollowGroupMetric_Count_Gauge.WithLabels(item.Group).Set(item.Events, item.Timestamp);
                _FollowGroupMetric_MaxElapsedMs_Gauge.WithLabels(item.Group).Set(item.MaxElapsedMs, item.Timestamp);
                _FollowGroupMetric_AvgElapsedMs_Gauge.WithLabels(item.Group).Set(item.AvgElapsedMs, item.Timestamp);
                _FollowGroupMetric_MinElapsedMs_Gauge.WithLabels(item.Group).Set(item.MinElapsedMs, item.Timestamp);
                _FollowGroupMetric_MaxDeliveryElapsedMs_Gauge.WithLabels(item.Group).Set(item.MaxDeliveryElapsedMs, item.Timestamp);
                _FollowGroupMetric_AvgDeliveryElapsedMs_Gauge.WithLabels(item.Group).Set(item.AvgDeliveryElapsedMs, item.Timestamp);
                _FollowGroupMetric_MinDeliveryElapsedMs_Gauge.WithLabels(item.Group).Set(item.MinDeliveryElapsedMs, item.Timestamp);
            }
        }
    }
}
