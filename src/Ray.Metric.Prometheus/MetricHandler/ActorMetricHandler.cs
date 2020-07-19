using System.Collections.Generic;
using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class ActorMetricHandler
    {
        private readonly Gauge ActorMetricMaxEventsPerActorGauge;
        private readonly Gauge ActorMetricAvgEventsPerActorGauge;
        private readonly Gauge ActorMetricMinEventsPerActorGauge;
        private readonly Gauge ActorMetricLivesGauge;
        private readonly Gauge ActorMetricIgnoresGauge;
        private readonly Gauge ActorMetricEventsGauge;

        public ActorMetricHandler()
        {
            this.ActorMetricMaxEventsPerActorGauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.MaxEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricAvgEventsPerActorGauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.AvgEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricMinEventsPerActorGauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.MinEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricLivesGauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Lives)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricIgnoresGauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Ignores)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricEventsGauge = Metrics.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Events)}", string.Empty, nameof(ActorMetric.Actor));
        }

        public void Handle(List<ActorMetric> actorMetrics)
        {
            foreach (var item in actorMetrics)
            {
                this.ActorMetricMaxEventsPerActorGauge.WithLabels(item.Actor).Set(item.MaxEventsPerActor, item.Timestamp);
                this.ActorMetricAvgEventsPerActorGauge.WithLabels(item.Actor).Set(item.AvgEventsPerActor, item.Timestamp);
                this.ActorMetricMinEventsPerActorGauge.WithLabels(item.Actor).Set(item.MinEventsPerActor, item.Timestamp);
                this.ActorMetricLivesGauge.WithLabels(item.Actor).Set(item.Lives, item.Timestamp);
                this.ActorMetricIgnoresGauge.WithLabels(item.Actor).Set(item.Ignores, item.Timestamp);
                this.ActorMetricEventsGauge.WithLabels(item.Actor).Set(item.Events, item.Timestamp);
            }
        }
    }
}
