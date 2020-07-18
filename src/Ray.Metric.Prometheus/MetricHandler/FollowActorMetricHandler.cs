using Prometheus.Client;
using Ray.Metric.Core.Element;
using System.Collections.Generic;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowActorMetricHandler
    {
        readonly Gauge _FollowActorMetric_Count_Gauge;
        readonly Gauge _FollowActorMetric_MaxElapsedMs_Gauge;
        readonly Gauge _FollowActorMetric_AvgElapsedMs_Gauge;
        readonly Gauge _FollowActorMetric_MinElapsedMs_Gauge;
        public FollowActorMetricHandler()
        {
            _FollowActorMetric_Count_Gauge = Metrics.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.Events)}", string.Empty, nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));
            _FollowActorMetric_MaxElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.MaxElapsedMs)}", string.Empty, nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));
            _FollowActorMetric_AvgElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.AvgElapsedMs)}", string.Empty, nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));
            _FollowActorMetric_MinElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(FollowActorMetric)}_{nameof(FollowActorMetric.MinElapsedMs)}", string.Empty, nameof(FollowActorMetric.Actor), nameof(FollowActorMetric.FromActor));
        }
        public void Handle(List<FollowActorMetric> followActorMetrics)
        {
            foreach (var item in followActorMetrics)
            {
                _FollowActorMetric_Count_Gauge.WithLabels(item.Actor, item.FromActor).Set(item.Events, item.Timestamp);
                _FollowActorMetric_MaxElapsedMs_Gauge.WithLabels(item.Actor, item.FromActor).Set(item.MaxElapsedMs, item.Timestamp);
                _FollowActorMetric_AvgElapsedMs_Gauge.WithLabels(item.Actor, item.FromActor).Set(item.AvgElapsedMs, item.Timestamp);
                _FollowActorMetric_MinElapsedMs_Gauge.WithLabels(item.Actor, item.FromActor).Set(item.MinElapsedMs, item.Timestamp);
            }
        }
    }
}
