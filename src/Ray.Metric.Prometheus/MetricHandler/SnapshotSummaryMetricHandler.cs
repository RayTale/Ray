using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class SnapshotSummaryMetricHandler
    {
        readonly Gauge _SnapshotSummaryMetric_SaveCount_Gauge;
        readonly Gauge _SnapshotSummaryMetric_MaxElapsedVersion_Gauge;
        readonly Gauge _SnapshotSummaryMetric_AvgElapsedVersion_Gauge;
        readonly Gauge _SnapshotSummaryMetric_MinElapsedVersion_Gauge;
        readonly Gauge _SnapshotSummaryMetric_MaxSaveElapsedMs_Gauge;
        readonly Gauge _SnapshotSummaryMetric_AvgSaveElapsedMs_Gauge;
        readonly Gauge _SnapshotSummaryMetric_MinSaveElapsedMs_Gauge;
        public SnapshotSummaryMetricHandler()
        {
            _SnapshotSummaryMetric_SaveCount_Gauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.SaveCount)}", string.Empty);
            _SnapshotSummaryMetric_MaxElapsedVersion_Gauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MaxElapsedVersion)}", string.Empty);
            _SnapshotSummaryMetric_AvgElapsedVersion_Gauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.AvgElapsedVersion)}", string.Empty);
            _SnapshotSummaryMetric_MinElapsedVersion_Gauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MinElapsedVersion)}", string.Empty);
            _SnapshotSummaryMetric_MaxSaveElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MaxSaveElapsedMs)}", string.Empty);
            _SnapshotSummaryMetric_AvgSaveElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.AvgSaveElapsedMs)}", string.Empty);
            _SnapshotSummaryMetric_MinSaveElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MinSaveElapsedMs)}", string.Empty);
        }
        public void Handle(SnapshotSummaryMetric snapshotSummaryMetrics)
        {
            _SnapshotSummaryMetric_SaveCount_Gauge.Set(snapshotSummaryMetrics.SaveCount, snapshotSummaryMetrics.Timestamp);
            _SnapshotSummaryMetric_MaxElapsedVersion_Gauge.Set(snapshotSummaryMetrics.MaxElapsedVersion, snapshotSummaryMetrics.Timestamp);
            _SnapshotSummaryMetric_AvgElapsedVersion_Gauge.Set(snapshotSummaryMetrics.AvgElapsedVersion, snapshotSummaryMetrics.Timestamp);
            _SnapshotSummaryMetric_MinElapsedVersion_Gauge.Set(snapshotSummaryMetrics.MinElapsedVersion, snapshotSummaryMetrics.Timestamp);
            _SnapshotSummaryMetric_MaxSaveElapsedMs_Gauge.Set(snapshotSummaryMetrics.MaxSaveElapsedMs, snapshotSummaryMetrics.Timestamp);
            _SnapshotSummaryMetric_AvgSaveElapsedMs_Gauge.Set(snapshotSummaryMetrics.AvgSaveElapsedMs, snapshotSummaryMetrics.Timestamp);
            _SnapshotSummaryMetric_MinSaveElapsedMs_Gauge.Set(snapshotSummaryMetrics.MinSaveElapsedMs, snapshotSummaryMetrics.Timestamp);
        }
    }
}
