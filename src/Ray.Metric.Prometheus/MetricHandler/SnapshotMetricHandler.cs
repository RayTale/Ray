using System.Collections.Generic;
using Prometheus.Client;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class SnapshotMetricHandler
    {
        private readonly Gauge SnapshotMetricSaveCountGauge;
        private readonly Gauge SnapshotMetricMaxElapsedVersionGauge;
        private readonly Gauge SnapshotMetricAvgElapsedVersionGauge;
        private readonly Gauge SnapshotMetricMinElapsedVersionGauge;
        private readonly Gauge SnapshotMetricMaxSaveElapsedMsGauge;
        private readonly Gauge SnapshotMetricAvgSaveElapsedMsGauge;
        private readonly Gauge SnapshotMetricMinSaveElapsedMsGauge;

        public SnapshotMetricHandler()
        {
            this.SnapshotMetricSaveCountGauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.SaveCount)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            this.SnapshotMetricMaxElapsedVersionGauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MaxElapsedVersion)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            this.SnapshotMetricAvgElapsedVersionGauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.AvgElapsedVersion)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            this.SnapshotMetricMinElapsedVersionGauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MinElapsedVersion)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            this.SnapshotMetricMaxSaveElapsedMsGauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MaxSaveElapsedMs)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            this.SnapshotMetricAvgSaveElapsedMsGauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.AvgSaveElapsedMs)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            this.SnapshotMetricMinSaveElapsedMsGauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MinSaveElapsedMs)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
        }

        public void Handle(List<SnapshotMetric> snapshotMetrics)
        {
            foreach (var item in snapshotMetrics)
            {
                this.SnapshotMetricSaveCountGauge.WithLabels(item.Actor, item.Snapshot).Set(item.SaveCount, item.Timestamp);
                this.SnapshotMetricMaxElapsedVersionGauge.WithLabels(item.Actor, item.Snapshot).Set(item.MaxElapsedVersion, item.Timestamp);
                this.SnapshotMetricAvgElapsedVersionGauge.WithLabels(item.Actor, item.Snapshot).Set(item.AvgElapsedVersion, item.Timestamp);
                this.SnapshotMetricMinElapsedVersionGauge.WithLabels(item.Actor, item.Snapshot).Set(item.MinElapsedVersion, item.Timestamp);
                this.SnapshotMetricMaxSaveElapsedMsGauge.WithLabels(item.Actor, item.Snapshot).Set(item.MaxSaveElapsedMs, item.Timestamp);
                this.SnapshotMetricAvgSaveElapsedMsGauge.WithLabels(item.Actor, item.Snapshot).Set(item.AvgSaveElapsedMs, item.Timestamp);
                this.SnapshotMetricMinSaveElapsedMsGauge.WithLabels(item.Actor, item.Snapshot).Set(item.MinSaveElapsedMs, item.Timestamp);
            }
        }
    }
}
