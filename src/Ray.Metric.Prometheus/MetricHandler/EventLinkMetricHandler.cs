using System.Collections.Generic;
using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventLinkMetricHandler
    {
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string ParentActor, string ParentEvent)> EventLinkMetricCountGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string ParentActor, string ParentEvent)> EventLinkMetricMaxElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string ParentActor, string ParentEvent)> EventLinkMetricMinElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string ParentActor, string ParentEvent)> EventLinkMetricAvgElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string ParentActor, string ParentEvent)> EventLinkMetricIgnoresGauge;

        public EventLinkMetricHandler(IMetricFactory metricFactory)
        {
            var labelNames = (nameof(EventLinkMetric.Actor), nameof(EventLinkMetric.Event), nameof(EventLinkMetric.ParentActor), nameof(EventLinkMetric.ParentEvent));
            this.EventLinkMetricCountGauge = metricFactory.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.Events)}", string.Empty, labelNames);
            this.EventLinkMetricMaxElapsedMsGauge = metricFactory.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.MaxElapsedMs)}", string.Empty, labelNames);
            this.EventLinkMetricMinElapsedMsGauge = metricFactory.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.MinElapsedMs)}", string.Empty, labelNames);
            this.EventLinkMetricAvgElapsedMsGauge = metricFactory.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.AvgElapsedMs)}", string.Empty, labelNames);
            this.EventLinkMetricIgnoresGauge = metricFactory.CreateGauge($"{nameof(EventLinkMetric)}_{nameof(EventLinkMetric.Ignores)}", string.Empty, labelNames);
        }

        public void Handle(List<EventLinkMetric> eventMetrics)
        {
            foreach (var item in eventMetrics)
            {
                var labels = (item.Actor, item.Event, item.ParentActor ?? string.Empty, item.ParentEvent ?? string.Empty);

                this.EventLinkMetricCountGauge.WithLabels(labels).Set(item.Events, item.Timestamp);
                this.EventLinkMetricMaxElapsedMsGauge.WithLabels(labels).Set(item.MaxElapsedMs, item.Timestamp);
                this.EventLinkMetricMinElapsedMsGauge.WithLabels(labels).Set(item.MinElapsedMs, item.Timestamp);
                this.EventLinkMetricAvgElapsedMsGauge.WithLabels(labels).Set(item.AvgElapsedMs, item.Timestamp);
                this.EventLinkMetricIgnoresGauge.WithLabels(labels).Set(item.Ignores, item.Timestamp);
            }
        }
    }
}
