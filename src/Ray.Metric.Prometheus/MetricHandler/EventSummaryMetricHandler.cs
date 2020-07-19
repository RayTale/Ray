using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventSummaryMetricHandler
    {
        private readonly Gauge EventSummaryMetricMaxEventsPerActorGauge;
        private readonly Gauge EventSummaryMetricAvgEventsPerActorGauge;
        private readonly Gauge EventSummaryMetricMinEventsPerActorGauge;
        private readonly Gauge EventSummaryMetricLivesGauge;
        private readonly Gauge EventSummaryMetricIgnoresGauge;
        private readonly Gauge EventSummaryMetricEventsGauge;

        public EventSummaryMetricHandler()
        {
            this.EventSummaryMetricMaxEventsPerActorGauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.MaxEventsPerActor)}", string.Empty);
            this.EventSummaryMetricAvgEventsPerActorGauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.AvgEventsPerActor)}", string.Empty);
            this.EventSummaryMetricMinEventsPerActorGauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.MinEventsPerActor)}", string.Empty);
            this.EventSummaryMetricLivesGauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.ActorLives)}", string.Empty);
            this.EventSummaryMetricIgnoresGauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.Ignores)}", string.Empty);
            this.EventSummaryMetricEventsGauge = Metrics.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.Events)}", string.Empty);
        }

        public void Handle(EventSummaryMetric summaryMetric)
        {
            this.EventSummaryMetricMaxEventsPerActorGauge.Set(summaryMetric.MaxEventsPerActor, summaryMetric.Timestamp);
            this.EventSummaryMetricAvgEventsPerActorGauge.Set(summaryMetric.AvgEventsPerActor, summaryMetric.Timestamp);
            this.EventSummaryMetricMinEventsPerActorGauge.Set(summaryMetric.MinEventsPerActor, summaryMetric.Timestamp);
            this.EventSummaryMetricLivesGauge.Set(summaryMetric.ActorLives, summaryMetric.Timestamp);
            this.EventSummaryMetricIgnoresGauge.Set(summaryMetric.Ignores, summaryMetric.Timestamp);
            this.EventSummaryMetricEventsGauge.Set(summaryMetric.Events, summaryMetric.Timestamp);
        }
    }
}
