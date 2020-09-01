using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class SnapshotSummaryMetricHandler
    {
        private readonly IGauge SnapshotSummaryMetricSaveCountGauge;
        private readonly IGauge SnapshotSummaryMetricMaxElapsedVersionGauge;
        private readonly IGauge SnapshotSummaryMetricAvgElapsedVersionGauge;
        private readonly IGauge SnapshotSummaryMetricMinElapsedVersionGauge;
        private readonly IGauge SnapshotSummaryMetricMaxSaveElapsedMsGauge;
        private readonly IGauge SnapshotSummaryMetricAvgSaveElapsedMsGauge;
        private readonly IGauge SnapshotSummaryMetricMinSaveElapsedMsGauge;

        public SnapshotSummaryMetricHandler(IMetricFactory metricFactory)
        {
            this.SnapshotSummaryMetricSaveCountGauge = metricFactory.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.SaveCount)}", string.Empty);
            this.SnapshotSummaryMetricMaxElapsedVersionGauge = metricFactory.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MaxElapsedVersion)}", string.Empty);
            this.SnapshotSummaryMetricAvgElapsedVersionGauge = metricFactory.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.AvgElapsedVersion)}", string.Empty);
            this.SnapshotSummaryMetricMinElapsedVersionGauge = metricFactory.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MinElapsedVersion)}", string.Empty);
            this.SnapshotSummaryMetricMaxSaveElapsedMsGauge = metricFactory.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MaxSaveElapsedMs)}", string.Empty);
            this.SnapshotSummaryMetricAvgSaveElapsedMsGauge = metricFactory.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.AvgSaveElapsedMs)}", string.Empty);
            this.SnapshotSummaryMetricMinSaveElapsedMsGauge = metricFactory.CreateGauge($"{nameof(SnapshotSummaryMetric)}_{nameof(SnapshotSummaryMetric.MinSaveElapsedMs)}", string.Empty);
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
