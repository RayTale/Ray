using Prometheus.Client;
using Ray.Metric.Core.Element;
using System.Collections.Generic;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventMetricHandler
    {
        readonly Gauge _Event_Count_Gauge;
        readonly Gauge _Event_MaxPerActor_Gauge;
        readonly Gauge _Event_AvgPerActor_Gauge;
        readonly Gauge _Event_MinPerActor_Gauge;
        readonly Gauge _Event_MaxInsertElapsedMs_Gauge;
        readonly Gauge _Event_AvgInsertElapsedMs_Gauge;
        readonly Gauge _Event_MinInsertElapsedMs_Gauge;
        readonly Gauge _Event_Ignores_Gauge;
        public EventMetricHandler()
        {
            _Event_Count_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.Events)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            _Event_MaxPerActor_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MaxPerActor)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            _Event_AvgPerActor_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.AvgPerActor)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            _Event_MinPerActor_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MinPerActor)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            _Event_MaxInsertElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MaxInsertElapsedMs)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            _Event_AvgInsertElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.AvgInsertElapsedMs)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            _Event_MinInsertElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MinInsertElapsedMs)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            _Event_Ignores_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.Ignores)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
        }
        public void Handle(List<EventMetric> eventMetrics)
        {
            foreach (var item in eventMetrics)
            {
                _Event_Count_Gauge.WithLabels(item.Actor, item.Event).Set(item.Events, item.Timestamp);
                _Event_MaxPerActor_Gauge.WithLabels(item.Actor, item.Event).Set(item.MaxPerActor, item.Timestamp);
                _Event_AvgPerActor_Gauge.WithLabels(item.Actor, item.Event).Set(item.AvgPerActor, item.Timestamp);
                _Event_MinPerActor_Gauge.WithLabels(item.Actor, item.Event).Set(item.MinPerActor, item.Timestamp);
                _Event_MaxInsertElapsedMs_Gauge.WithLabels(item.Actor, item.Event).Set(item.MaxInsertElapsedMs, item.Timestamp);
                _Event_AvgInsertElapsedMs_Gauge.WithLabels(item.Actor, item.Event).Set(item.AvgInsertElapsedMs, item.Timestamp);
                _Event_MinInsertElapsedMs_Gauge.WithLabels(item.Actor, item.Event).Set(item.MinInsertElapsedMs, item.Timestamp);
                _Event_Ignores_Gauge.WithLabels(item.Actor, item.Event).Set(item.Ignores, item.Timestamp);
            }
        }
    }
}
