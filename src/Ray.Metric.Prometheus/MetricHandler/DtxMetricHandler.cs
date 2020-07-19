using System.Collections.Generic;
using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class DtxMetricHandler
    {
        private readonly Gauge DtxMetricMaxElapsedMsGauge;
        private readonly Gauge DtxMetricAvgElapsedMsGauge;
        private readonly Gauge DtxMetricMinElapsedMsGauge;
        private readonly Gauge DtxMetricTimesGauge;
        private readonly Gauge DtxMetricCommitsGauge;
        private readonly Gauge DtxMetricRollbacksGauge;

        public DtxMetricHandler()
        {
            this.DtxMetricMaxElapsedMsGauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.MaxElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricAvgElapsedMsGauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.AvgElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricMinElapsedMsGauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.MinElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricTimesGauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Times)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricCommitsGauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Commits)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricRollbacksGauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Rollbacks)}", string.Empty, nameof(DtxMetric.Actor));
        }

        public void Handle(List<DtxMetric> DtxMetrics)
        {
            foreach (var item in DtxMetrics)
            {
                this.DtxMetricMaxElapsedMsGauge.WithLabels(item.Actor).Set(item.MaxElapsedMs, item.Timestamp);
                this.DtxMetricAvgElapsedMsGauge.WithLabels(item.Actor).Set(item.AvgElapsedMs, item.Timestamp);
                this.DtxMetricMinElapsedMsGauge.WithLabels(item.Actor).Set(item.MinElapsedMs, item.Timestamp);
                this.DtxMetricTimesGauge.WithLabels(item.Actor).Set(item.Times, item.Timestamp);
                this.DtxMetricCommitsGauge.WithLabels(item.Actor).Set(item.Commits, item.Timestamp);
                this.DtxMetricRollbacksGauge.WithLabels(item.Actor).Set(item.Rollbacks, item.Timestamp);
            }
        }
    }
}
