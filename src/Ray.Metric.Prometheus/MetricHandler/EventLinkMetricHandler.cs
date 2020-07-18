using System.Collections.Generic;
using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventLinkMetricHandler
    {
        private readonly Gauge EventLinkMetricCountGauge;
        private readonly Gauge EventLinkMetricMaxElapsedMsGauge;
        private readonly Gauge EventLinkMetricMinElapsedMsGauge;
        private readonly Gauge EventLinkMetricAvgElapsedMsGauge;
        private readonly Gauge EventLinkMetricIgnoresGauge;

        public EventLinkMetricHandler()
        {
            this.EventLinkMetricCountGauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.Events)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            this.EventLinkMetricMaxElapsedMsGauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.MaxElapsedMs)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            this.EventLinkMetricMinElapsedMsGauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.MinElapsedMs)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            this.EventLinkMetricAvgElapsedMsGauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.AvgElapsedMs)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            this.EventLinkMetricIgnoresGauge = Metrics.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.Ignores)}", string.Empty, nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
        }

        public void Handle(List<EventLinkMetric> eventMetrics)
        {
            foreach (var item in eventMetrics)
            {
                this.EventLinkMetricCountGauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.Events, item.Timestamp);
                this.EventLinkMetricMaxElapsedMsGauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.MaxElapsedMs, item.Timestamp);
                this.EventLinkMetricMinElapsedMsGauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.MinElapsedMs, item.Timestamp);
                this.EventLinkMetricAvgElapsedMsGauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.AvgElapsedMs, item.Timestamp);
                this.EventLinkMetricIgnoresGauge.WithLabels(item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty).Set(item.Ignores, item.Timestamp);
            }
        }
    }
}
