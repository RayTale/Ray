using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class DtxSummaryMetricHandler
    {
        private readonly IGauge DtxSummaryMetricMaxElapsedMsGauge;
        private readonly IGauge DtxSummaryMetricAvgElapsedMsGauge;
        private readonly IGauge DtxSummaryMetricMinElapsedMsGauge;
        private readonly IGauge DtxSummaryMetricTimesGauge;
        private readonly IGauge DtxSummaryMetricCommitsGauge;
        private readonly IGauge DtxSummaryMetricRollbacksGauge;

        public DtxSummaryMetricHandler(IMetricFactory metricFactory)
        {
            this.DtxSummaryMetricMaxElapsedMsGauge = metricFactory.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.MaxElapsedMs)}", string.Empty);
            this.DtxSummaryMetricAvgElapsedMsGauge = metricFactory.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.AvgElapsedMs)}", string.Empty);
            this.DtxSummaryMetricMinElapsedMsGauge = metricFactory.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.MinElapsedMs)}", string.Empty);
            this.DtxSummaryMetricTimesGauge = metricFactory.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Times)}", string.Empty);
            this.DtxSummaryMetricCommitsGauge = metricFactory.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Commits)}", string.Empty);
            this.DtxSummaryMetricRollbacksGauge = metricFactory.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Rollbacks)}", string.Empty);
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
