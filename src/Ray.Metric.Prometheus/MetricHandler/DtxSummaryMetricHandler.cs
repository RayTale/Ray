using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class DtxSummaryMetricHandler
    {
        readonly Gauge _DtxSummaryMetric_MaxElapsedMs_Gauge;
        readonly Gauge _DtxSummaryMetric_AvgElapsedMs_Gauge;
        readonly Gauge _DtxSummaryMetric_MinElapsedMs_Gauge;
        readonly Gauge _DtxSummaryMetric_Times_Gauge;
        readonly Gauge _DtxSummaryMetric_Commits_Gauge;
        readonly Gauge _DtxSummaryMetric_Rollbacks_Gauge;
        public DtxSummaryMetricHandler()
        {
            _DtxSummaryMetric_MaxElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.MaxElapsedMs)}", string.Empty);
            _DtxSummaryMetric_AvgElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.AvgElapsedMs)}", string.Empty);
            _DtxSummaryMetric_MinElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.MinElapsedMs)}", string.Empty);
            _DtxSummaryMetric_Times_Gauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Times)}", string.Empty);
            _DtxSummaryMetric_Commits_Gauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Commits)}", string.Empty);
            _DtxSummaryMetric_Rollbacks_Gauge = Metrics.CreateGauge($"{nameof(DtxSummaryMetric)}_{nameof(DtxSummaryMetric.Rollbacks)}", string.Empty);
        }
        public void Handle(DtxSummaryMetric dtxSummaryMetrics)
        {
            _DtxSummaryMetric_MaxElapsedMs_Gauge.Set(dtxSummaryMetrics.MaxElapsedMs, dtxSummaryMetrics.Timestamp);
            _DtxSummaryMetric_AvgElapsedMs_Gauge.Set(dtxSummaryMetrics.AvgElapsedMs, dtxSummaryMetrics.Timestamp);
            _DtxSummaryMetric_MinElapsedMs_Gauge.Set(dtxSummaryMetrics.MinElapsedMs, dtxSummaryMetrics.Timestamp);
            _DtxSummaryMetric_Times_Gauge.Set(dtxSummaryMetrics.Times, dtxSummaryMetrics.Timestamp);
            _DtxSummaryMetric_Commits_Gauge.Set(dtxSummaryMetrics.Commits, dtxSummaryMetrics.Timestamp);
            _DtxSummaryMetric_Rollbacks_Gauge.Set(dtxSummaryMetrics.Rollbacks, dtxSummaryMetrics.Timestamp);
        }
    }
}
