using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventSummaryMetricHandler
    {
        private readonly IGauge EventSummaryMetricMaxEventsPerActorGauge;
        private readonly IGauge EventSummaryMetricAvgEventsPerActorGauge;
        private readonly IGauge EventSummaryMetricMinEventsPerActorGauge;
        private readonly IGauge EventSummaryMetricLivesGauge;
        private readonly IGauge EventSummaryMetricIgnoresGauge;
        private readonly IGauge EventSummaryMetricEventsGauge;

        public EventSummaryMetricHandler(IMetricFactory metricFactory)
        {
            this.EventSummaryMetricMaxEventsPerActorGauge = metricFactory.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.MaxEventsPerActor)}", string.Empty);
            this.EventSummaryMetricAvgEventsPerActorGauge = metricFactory.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.AvgEventsPerActor)}", string.Empty);
            this.EventSummaryMetricMinEventsPerActorGauge = metricFactory.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.MinEventsPerActor)}", string.Empty);
            this.EventSummaryMetricLivesGauge = metricFactory.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.ActorLives)}", string.Empty);
            this.EventSummaryMetricIgnoresGauge = metricFactory.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.Ignores)}", string.Empty);
            this.EventSummaryMetricEventsGauge = metricFactory.CreateGauge($"{nameof(EventSummaryMetric)}_{nameof(EventSummaryMetric.Events)}", string.Empty);
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
