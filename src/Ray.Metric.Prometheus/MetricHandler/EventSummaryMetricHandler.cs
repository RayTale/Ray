using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventSummaryMetricHandler
    {
        readonly Gauge _EventSummaryMetric_MaxEventsPerActor_Gauge;
        readonly Gauge _EventSummaryMetric_AvgEventsPerActor_Gauge;
        readonly Gauge _EventSummaryMetric_MinEventsPerActor_Gauge;
        readonly Gauge _EventSummaryMetric_Lives_Gauge;
        readonly Gauge _EventSummaryMetric_Ignores_Gauge;
        readonly Gauge _EventSummaryMetric_Events_Gauge;
        public EventSummaryMetricHandler()
        {
            _EventSummaryMetric_MaxEventsPerActor_Gauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.MaxEventsPerActor)}", string.Empty);
            _EventSummaryMetric_AvgEventsPerActor_Gauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.AvgEventsPerActor)}", string.Empty);
            _EventSummaryMetric_MinEventsPerActor_Gauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.MinEventsPerActor)}", string.Empty);
            _EventSummaryMetric_Lives_Gauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.ActorLives)}", string.Empty);
            _EventSummaryMetric_Ignores_Gauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.Ignores)}", string.Empty);
            _EventSummaryMetric_Events_Gauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.Events)}", string.Empty);
        }
        public void Handle(EventSummaryMetric summaryMetric)
        {
            _EventSummaryMetric_MaxEventsPerActor_Gauge.Set(summaryMetric.MaxEventsPerActor, summaryMetric.Timestamp);
            _EventSummaryMetric_AvgEventsPerActor_Gauge.Set(summaryMetric.AvgEventsPerActor, summaryMetric.Timestamp);
            _EventSummaryMetric_MinEventsPerActor_Gauge.Set(summaryMetric.MinEventsPerActor, summaryMetric.Timestamp);
            _EventSummaryMetric_Lives_Gauge.Set(summaryMetric.ActorLives, summaryMetric.Timestamp);
            _EventSummaryMetric_Ignores_Gauge.Set(summaryMetric.Ignores, summaryMetric.Timestamp);
            _EventSummaryMetric_Events_Gauge.Set(summaryMetric.Events, summaryMetric.Timestamp);
        }
    }
}
