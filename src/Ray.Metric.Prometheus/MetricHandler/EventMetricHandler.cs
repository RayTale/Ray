using System.Collections.Generic;
using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class EventMetricHandler
    {
        private readonly Gauge EventCountGauge;
        private readonly Gauge EventMaxPerActorGauge;
        private readonly Gauge EventAvgPerActorGauge;
        private readonly Gauge EventMinPerActorGauge;
        private readonly Gauge EventMaxInsertElapsedMsGauge;
        private readonly Gauge EventAvgInsertElapsedMsGauge;
        private readonly Gauge EventMinInsertElapsedMsGauge;
        private readonly Gauge EventIgnoresGauge;

        public EventMetricHandler()
        {
            this.EventCountGauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.Events)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            this.EventMaxPerActorGauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MaxPerActor)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            this.EventAvgPerActorGauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.AvgPerActor)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            this.EventMinPerActorGauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MinPerActor)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            this.EventMaxInsertElapsedMsGauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MaxInsertElapsedMs)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            this.EventAvgInsertElapsedMsGauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.AvgInsertElapsedMs)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            this.EventMinInsertElapsedMsGauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.MinInsertElapsedMs)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
            this.EventIgnoresGauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.Ignores)}", string.Empty, nameof(EventMetric.Actor), nameof(EventMetric.Event));
        }

        public void Handle(List<EventMetric> eventMetrics)
        {
            foreach (var item in eventMetrics)
            {
                this.EventCountGauge.WithLabels(item.Actor, item.Event).Set(item.Events, item.Timestamp);
                this.EventMaxPerActorGauge.WithLabels(item.Actor, item.Event).Set(item.MaxPerActor, item.Timestamp);
                this.EventAvgPerActorGauge.WithLabels(item.Actor, item.Event).Set(item.AvgPerActor, item.Timestamp);
                this.EventMinPerActorGauge.WithLabels(item.Actor, item.Event).Set(item.MinPerActor, item.Timestamp);
                this.EventMaxInsertElapsedMsGauge.WithLabels(item.Actor, item.Event).Set(item.MaxInsertElapsedMs, item.Timestamp);
                this.EventAvgInsertElapsedMsGauge.WithLabels(item.Actor, item.Event).Set(item.AvgInsertElapsedMs, item.Timestamp);
                this.EventMinInsertElapsedMsGauge.WithLabels(item.Actor, item.Event).Set(item.MinInsertElapsedMs, item.Timestamp);
                this.EventIgnoresGauge.WithLabels(item.Actor, item.Event).Set(item.Ignores, item.Timestamp);
            }
        }
    }
}
