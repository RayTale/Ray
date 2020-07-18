using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class SnapshotSummaryMetricHandler
    {
        private readonly Gauge SnapshotSummaryMetricSaveCountGauge;
        private readonly Gauge SnapshotSummaryMetricMaxElapsedVersionGauge;
        private readonly Gauge SnapshotSummaryMetricAvgElapsedVersionGauge;
        private readonly Gauge SnapshotSummaryMetricMinElapsedVersionGauge;
        private readonly Gauge SnapshotSummaryMetricMaxSaveElapsedMsGauge;
        private readonly Gauge SnapshotSummaryMetricAvgSaveElapsedMsGauge;
        private readonly Gauge SnapshotSummaryMetricMinSaveElapsedMsGauge;

        public SnapshotSummaryMetricHandler()
        {
            this.SnapshotSummaryMetricSaveCountGauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.SaveCount)}", string.Empty);
            this.SnapshotSummaryMetricMaxElapsedVersionGauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MaxElapsedVersion)}", string.Empty);
            this.SnapshotSummaryMetricAvgElapsedVersionGauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.AvgElapsedVersion)}", string.Empty);
            this.SnapshotSummaryMetricMinElapsedVersionGauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MinElapsedVersion)}", string.Empty);
            this.SnapshotSummaryMetricMaxSaveElapsedMsGauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MaxSaveElapsedMs)}", string.Empty);
            this.SnapshotSummaryMetricAvgSaveElapsedMsGauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.AvgSaveElapsedMs)}", string.Empty);
            this.SnapshotSummaryMetricMinSaveElapsedMsGauge = Metrics.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MinSaveElapsedMs)}", string.Empty);
        }

        public void Handle(SnapshotSummaryMetric snapshotSummaryMetrics)
        {
            this.SnapshotSummaryMetricSaveCountGauge.Set(snapshotSummaryMetrics.SaveCount, snapshotSummaryMetrics.Timestamp);
            this.SnapshotSummaryMetricMaxElapsedVersionGauge.Set(snapshotSummaryMetrics.MaxElapsedVersion, snapshotSummaryMetrics.Timestamp);
            this.SnapshotSummaryMetricAvgElapsedVersionGauge.Set(snapshotSummaryMetrics.AvgElapsedVersion, snapshotSummaryMetrics.Timestamp);
            this.SnapshotSummaryMetricMinElapsedVersionGauge.Set(snapshotSummaryMetrics.MinElapsedVersion, snapshotSummaryMetrics.Timestamp);
            this.SnapshotSummaryMetricMaxSaveElapsedMsGauge.Set(snapshotSummaryMetrics.MaxSaveElapsedMs, snapshotSummaryMetrics.Timestamp);
            this.SnapshotSummaryMetricAvgSaveElapsedMsGauge.Set(snapshotSummaryMetrics.AvgSaveElapsedMs, snapshotSummaryMetrics.Timestamp);
            this.SnapshotSummaryMetricMinSaveElapsedMsGauge.Set(snapshotSummaryMetrics.MinSaveElapsedMs, snapshotSummaryMetrics.Timestamp);
        }
    }
}
