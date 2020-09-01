using System.Collections.Generic;
using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventMetricHandler
    {
        private readonly IMetricFamily<IGauge, (string Actor, string Event)> EventCountGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event)> EventMaxPerActorGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event)> EventAvgPerActorGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event)> EventMinPerActorGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event)> EventMaxInsertElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event)> EventAvgInsertElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event)> EventMinInsertElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event)> EventIgnoresGauge;

        public EventMetricHandler(IMetricFactory metricFactory)
        {
            var labelNames = (nameof(EventMetric.Actor), nameof(EventMetric.Event));

            this.EventCountGauge = metricFactory.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.Events)}", string.Empty, labelNames);
            this.EventMaxPerActorGauge = metricFactory.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MaxPerActor)}", string.Empty, labelNames);
            this.EventAvgPerActorGauge = metricFactory.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.AvgPerActor)}", string.Empty, labelNames);
            this.EventMinPerActorGauge = metricFactory.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MinPerActor)}", string.Empty, labelNames);
            this.EventMaxInsertElapsedMsGauge = metricFactory.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MaxInsertElapsedMs)}", string.Empty, labelNames);
            this.EventAvgInsertElapsedMsGauge = metricFactory.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.AvgInsertElapsedMs)}", string.Empty, labelNames);
            this.EventMinInsertElapsedMsGauge = metricFactory.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MinInsertElapsedMs)}", string.Empty, labelNames);
            this.EventIgnoresGauge = metricFactory.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.Ignores)}", string.Empty, labelNames);
        }

        public void Handle(List<EventMetric> eventMetrics)
        {
            foreach (var item in eventMetrics)
            {
                var labels = (item.Actor, item.Event);
                
                this.EventCountGauge.WithLabels(labels).Set(item.Events, item.Timestamp);
                this.EventMaxPerActorGauge.WithLabels(labels).Set(item.MaxPerActor, item.Timestamp);
                this.EventAvgPerActorGauge.WithLabels(labels).Set(item.AvgPerActor, item.Timestamp);
                this.EventMinPerActorGauge.WithLabels(labels).Set(item.MinPerActor, item.Timestamp);
                this.EventMaxInsertElapsedMsGauge.WithLabels(labels).Set(item.MaxInsertElapsedMs, item.Timestamp);
                this.EventAvgInsertElapsedMsGauge.WithLabels(labels).Set(item.AvgInsertElapsedMs, item.Timestamp);
                this.EventMinInsertElapsedMsGauge.WithLabels(labels).Set(item.MinInsertElapsedMs, item.Timestamp);
                this.EventIgnoresGauge.WithLabels(labels).Set(item.Ignores, item.Timestamp);
            }
        }
    }
}
