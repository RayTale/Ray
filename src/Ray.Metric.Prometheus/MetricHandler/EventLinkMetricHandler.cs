using Prometheus.Client;
using Ray.Metric.Core.Element;
using System.Collections.Generic;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventLinkMetricHandler
    {
        readonly Gauge _EventLinkMetric_Count_Gauge;
        readonly Gauge _EventLinkMetric_MaxElapsedMs_Gauge;
        readonly Gauge _EventLinkMetric_MinElapsedMs_Gauge;
        readonly Gauge _EventLinkMetric_AvgElapsedMs_Gauge;
        readonly Gauge _EventLinkMetric_Ignores_Gauge;
        public EventLinkMetricHandler()
        {
            _EventLinkMetric_Count_Gauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.Events)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            _EventLinkMetric_MaxElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.MaxElapsedMs)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            _EventLinkMetric_MinElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.MinElapsedMs)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            _EventLinkMetric_AvgElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.AvgElapsedMs)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            _EventLinkMetric_Ignores_Gauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.Ignores)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
        }
        public void Handle(List<EventLinkMetric> eventMetrics)
        {
            foreach (var item in eventMetrics)
            {
                _EventLinkMetric_Count_Gauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.Events, item.Timestamp);
                _EventLinkMetric_MaxElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.MaxElapsedMs, item.Timestamp);
                _EventLinkMetric_MinElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.MinElapsedMs, item.Timestamp);
                _EventLinkMetric_AvgElapsedMs_Gauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.AvgElapsedMs, item.Timestamp);
                _EventLinkMetric_Ignores_Gauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.Ignores, item.Timestamp);
            }
        }
    }
}
