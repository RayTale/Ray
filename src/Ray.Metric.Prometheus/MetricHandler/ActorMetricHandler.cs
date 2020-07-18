using Prometheus.Client;
using Ray.Metric.Core.Element;
using System.Collections.Generic;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class ActorMetricHandler
    {
        readonly Gauge _ActorMetric_MaxEventsPerActor_Gauge;
        readonly Gauge _ActorMetric_AvgEventsPerActor_Gauge;
        readonly Gauge _ActorMetric_MinEventsPerActor_Gauge;
        readonly Gauge _ActorMetric_Lives_Gauge;
        readonly Gauge _ActorMetric_Ignores_Gauge;
        readonly Gauge _ActorMetric_Events_Gauge;
        public ActorMetricHandler()
        {
            _ActorMetric_MaxEventsPerActor_Gauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.MaxEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            _ActorMetric_AvgEventsPerActor_Gauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.AvgEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            _ActorMetric_MinEventsPerActor_Gauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.MinEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            _ActorMetric_Lives_Gauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Lives)}", string.Empty, nameof(ActorMetric.Actor));
            _ActorMetric_Ignores_Gauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Ignores)}", string.Empty, nameof(ActorMetric.Actor));
            _ActorMetric_Events_Gauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Events)}", string.Empty, nameof(ActorMetric.Actor));
        }
        public void Handle(List<ActorMetric> actorMetrics)
        {
            foreach (var item in actorMetrics)
            {
                _ActorMetric_MaxEventsPerActor_Gauge.WithLabels(item.Actor).Set(item.MaxEventsPerActor, item.Timestamp);
                _ActorMetric_AvgEventsPerActor_Gauge.WithLabels(item.Actor).Set(item.AvgEventsPerActor, item.Timestamp);
                _ActorMetric_MinEventsPerActor_Gauge.WithLabels(item.Actor).Set(item.MinEventsPerActor, item.Timestamp);
                _ActorMetric_Lives_Gauge.WithLabels(item.Actor).Set(item.Lives, item.Timestamp);
                _ActorMetric_Ignores_Gauge.WithLabels(item.Actor).Set(item.Ignores, item.Timestamp);
                _ActorMetric_Events_Gauge.WithLabels(item.Actor).Set(item.Events, item.Timestamp);
            }
        }
    }
}
