using System.Collections.Generic;
using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class SnapshotMetricHandler
    {
        private readonly IMetricFamily<IGauge, (string Actor, string Snapshot)> SnapshotMetricSaveCountGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Snapshot)> SnapshotMetricMaxElapsedVersionGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Snapshot)> SnapshotMetricAvgElapsedVersionGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Snapshot)> SnapshotMetricMinElapsedVersionGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Snapshot)> SnapshotMetricMaxSaveElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Snapshot)> SnapshotMetricAvgSaveElapsedMsGauge;
        private readonly IMetricFamily<IGauge, (string Actor, string Snapshot)> SnapshotMetricMinSaveElapsedMsGauge;

        public SnapshotMetricHandler(IMetricFactory metricFactory)
        {
            var labelNames = (nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            
            this.SnapshotMetricSaveCountGauge = metricFactory.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.SaveCount)}", string.Empty, labelNames);
            this.SnapshotMetricMaxElapsedVersionGauge = metricFactory.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MaxElapsedVersion)}", string.Empty, labelNames);
            this.SnapshotMetricAvgElapsedVersionGauge = metricFactory.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.AvgElapsedVersion)}", string.Empty, labelNames);
            this.SnapshotMetricMinElapsedVersionGauge = metricFactory.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MinElapsedVersion)}", string.Empty, labelNames);
            this.SnapshotMetricMaxSaveElapsedMsGauge = metricFactory.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MaxSaveElapsedMs)}", string.Empty, labelNames);
            this.SnapshotMetricAvgSaveElapsedMsGauge = metricFactory.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.AvgSaveElapsedMs)}", string.Empty, labelNames);
            this.SnapshotMetricMinSaveElapsedMsGauge = metricFactory.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MinSaveElapsedMs)}", string.Empty, labelNames);
        }

        public void Handle(List<SnapshotMetric> snapshotMetrics)
        {
            foreach (var item in snapshotMetrics)
            {
                var labels = (item.Actor, item.Snapshot);
                
                this.SnapshotMetricSaveCountGauge.WithLabels(labels).Set(item.SaveCount, item.Timestamp);
                this.SnapshotMetricMaxElapsedVersionGauge.WithLabels(labels).Set(item.MaxElapsedVersion, item.Timestamp);
                this.SnapshotMetricAvgElapsedVersionGauge.WithLabels(labels).Set(item.AvgElapsedVersion, item.Timestamp);
                this.SnapshotMetricMinElapsedVersionGauge.WithLabels(labels).Set(item.MinElapsedVersion, item.Timestamp);
                this.SnapshotMetricMaxSaveElapsedMsGauge.WithLabels(labels).Set(item.MaxSaveElapsedMs, item.Timestamp);
                this.SnapshotMetricAvgSaveElapsedMsGauge.WithLabels(labels).Set(item.AvgSaveElapsedMs, item.Timestamp);
                this.SnapshotMetricMinSaveElapsedMsGauge.WithLabels(labels).Set(item.MinSaveElapsedMs, item.Timestamp);
            }
        }
    }
}
