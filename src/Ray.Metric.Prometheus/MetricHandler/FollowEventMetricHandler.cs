using Prometheus.Client;
using Ray.Metric.Core.Element;
using System.Collections.Generic;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowFollowEventMetricHandler
    {
        readonly Gauge _FollowEventMetric_Count_Gauge;
        readonly Gauge _FollowEventMetric_MaxElapsedMs_Gauge;
        readonly Gauge _FollowEventMetric_AvgElapsedMs_Gauge;
        readonly Gauge _FollowEventMetric_MinElapsedMs_Gauge;
        readonly Gauge _FollowEventMetric_MaxDeliveryElapsedMs_Gauge;
        readonly Gauge _FollowEventMetric_AvgDeliveryElapsedMs_Gauge;
        readonly Gauge _FollowEventMetric_MinDeliveryElapsedMs_Gauge;
        public FollowFollowEventMetricHandler()
        {
            _FollowEventMetric_Count_Gauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.Events)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            _FollowEventMetric_MaxElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MaxElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            _FollowEventMetric_AvgElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.AvgElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            _FollowEventMetric_MinElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MinElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            _FollowEventMetric_MaxDeliveryElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MaxDeliveryElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            _FollowEventMetric_AvgDeliveryElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.AvgDeliveryElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            _FollowEventMetric_MinDeliveryElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MinDeliveryElapsedMs)}", string.Empty, nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
        }
        public void Handle(List<FollowEventMetric> followEventMetrics)
        {
            foreach (var item in followEventMetrics)
            {
                _FollowEventMetric_Count_Gauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.Events, item.Timestamp);
                _FollowEventMetric_MaxElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.MaxElapsedMs, item.Timestamp);
                _FollowEventMetric_AvgElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.AvgElapsedMs, item.Timestamp);
                _FollowEventMetric_MinElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.MinElapsedMs, item.Timestamp);
                _FollowEventMetric_MaxDeliveryElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.MaxDeliveryElapsedMs, item.Timestamp);
                _FollowEventMetric_AvgDeliveryElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.AvgDeliveryElapsedMs, item.Timestamp);
                _FollowEventMetric_MinDeliveryElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.FromActor).Set(item.MinDeliveryElapsedMs, item.Timestamp);
            }
        }
    }
}
