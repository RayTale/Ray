﻿using System;
using System.Collections.Generic;
using Prometheus.Client;
using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class ActorMetricHandler
    {
        private readonly IMetricFamily<IGauge, ValueTuple<string>> ActorMetricMaxEventsPerActorGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> ActorMetricAvgEventsPerActorGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> ActorMetricMinEventsPerActorGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> ActorMetricLivesGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> ActorMetricIgnoresGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> ActorMetricEventsGauge;

        public ActorMetricHandler(IMetricFactory metricFactory)
        {
            this.ActorMetricMaxEventsPerActorGauge = metricFactory.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.MaxEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricAvgEventsPerActorGauge = metricFactory.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.AvgEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricMinEventsPerActorGauge = metricFactory.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.MinEventsPerActor)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricLivesGauge = metricFactory.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Lives)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricIgnoresGauge = metricFactory.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Ignores)}", string.Empty, nameof(ActorMetric.Actor));
            this.ActorMetricEventsGauge = metricFactory.CreateGauge($"{nameof(ActorMetric)}_{nameof(ActorMetric.Events)}", string.Empty, nameof(ActorMetric.Actor));
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
