using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class DtxSummaryMetricHandler
    {
        private readonly Gauge DtxSummaryMetricMaxElapsedMsGauge;
        private readonly Gauge DtxSummaryMetricAvgElapsedMsGauge;
        private readonly Gauge DtxSummaryMetricMinElapsedMsGauge;
        private readonly Gauge DtxSummaryMetricTimesGauge;
        private readonly Gauge DtxSummaryMetricCommitsGauge;
        private readonly Gauge DtxSummaryMetricRollbacksGauge;

        public DtxSummaryMetricHandler()
        {
            this.DtxSummaryMetricMaxElapsedMsGauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.MaxElapsedMs)}", string.Empty);
            this.DtxSummaryMetricAvgElapsedMsGauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.AvgElapsedMs)}", string.Empty);
            this.DtxSummaryMetricMinElapsedMsGauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.MinElapsedMs)}", string.Empty);
            this.DtxSummaryMetricTimesGauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Times)}", string.Empty);
            this.DtxSummaryMetricCommitsGauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Commits)}", string.Empty);
            this.DtxSummaryMetricRollbacksGauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Rollbacks)}", string.Empty);
        }

        public void Handle(DtxSummaryMetric dtxSummaryMetrics)
        {
            this.DtxSummaryMetricMaxElapsedMsGauge.Set(dtxSummaryMetrics.MaxElapsedMs, dtxSummaryMetrics.Timestamp);
            this.DtxSummaryMetricAvgElapsedMsGauge.Set(dtxSummaryMetrics.AvgElapsedMs, dtxSummaryMetrics.Timestamp);
            this.DtxSummaryMetricMinElapsedMsGauge.Set(dtxSummaryMetrics.MinElapsedMs, dtxSummaryMetrics.Timestamp);
            this.DtxSummaryMetricTimesGauge.Set(dtxSummaryMetrics.Times, dtxSummaryMetrics.Timestamp);
            this.DtxSummaryMetricCommitsGauge.Set(dtxSummaryMetrics.Commits, dtxSummaryMetrics.Timestamp);
            this.DtxSummaryMetricRollbacksGauge.Set(dtxSummaryMetrics.Rollbacks, dtxSummaryMetrics.Timestamp);
        }
    }
}
