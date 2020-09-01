using System.Collections.Generic;
using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class FollowFollowEventMetricHandler
    {
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string FromActor)> FollowEventMetricCountGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string FromActor)> FollowEventMetricMaxElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string FromActor)> FollowEventMetricAvgElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string FromActor)> FollowEventMetricMinElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string FromActor)> FollowEventMetricMaxDeliveryElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string FromActor)> FollowEventMetricAvgDeliveryElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Event, string FromActor)> FollowEventMetricMinDeliveryElapsedMsGauge;

        public FollowFollowEventMetricHandler(IMetricFactory metricFactory)
        {
            var labelNames = (nameof(FollowEventMetric.Actor), nameof(FollowEventMetric.Event), nameof(FollowEventMetric.FromActor));
            
            this.FollowEventMetricCountGauge = metricFactory.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.Events)}", string.Empty, labelNames);
            this.FollowEventMetricMaxElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MaxElapsedMs)}", string.Empty, labelNames);
            this.FollowEventMetricAvgElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.AvgElapsedMs)}", string.Empty, labelNames);
            this.FollowEventMetricMinElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MinElapsedMs)}", string.Empty, labelNames);
            this.FollowEventMetricMaxDeliveryElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MaxDeliveryElapsedMs)}", string.Empty, labelNames);
            this.FollowEventMetricAvgDeliveryElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.AvgDeliveryElapsedMs)}", string.Empty, labelNames);
            this.FollowEventMetricMinDeliveryElapsedMsGauge = metricFactory.CreateGauge($"{nameof(FollowEventMetric)}_{nameof(FollowEventMetric.MinDeliveryElapsedMs)}", string.Empty, labelNames);
        }

        public void Handle(List<FollowEventMetric> followEventMetrics)
        {
            foreach (var item in followEventMetrics)
            {
                var labels = (item.Actor, item.Event, item.FromActor);
                
                this.FollowEventMetricCountGauge.WithLabels(labels).Set(item.Events, item.Timestamp);
                this.FollowEventMetricMaxElapsedMsGauge.WithLabels(labels).Set(item.MaxElapsedMs, item.Timestamp);
                this.FollowEventMetricAvgElapsedMsGauge.WithLabels(labels).Set(item.AvgElapsedMs, item.Timestamp);
                this.FollowEventMetricMinElapsedMsGauge.WithLabels(labels).Set(item.MinElapsedMs, item.Timestamp);
                this.FollowEventMetricMaxDeliveryElapsedMsGauge.WithLabels(labels).Set(item.MaxDeliveryElapsedMs, item.Timestamp);
                this.FollowEventMetricAvgDeliveryElapsedMsGauge.WithLabels(labels).Set(item.AvgDeliveryElapsedMs, item.Timestamp);
                this.FollowEventMetricMinDeliveryElapsedMsGauge.WithLabels(labels).Set(item.MinDeliveryElapsedMs, item.Timestamp);
            }
        }
    }
}
