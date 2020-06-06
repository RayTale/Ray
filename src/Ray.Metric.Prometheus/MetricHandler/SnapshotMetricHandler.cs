using Prometheus.Client;
using Ray.Metric.Core.Element;
using System.Collections.Generic;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class SnapshotMetricHandler
    {
        readonly Gauge _SnapshotMetric_SaveCount_Gauge;
        readonly Gauge _SnapshotMetric_MaxElapsedVersion_Gauge;
        readonly Gauge _SnapshotMetric_AvgElapsedVersion_Gauge;
        readonly Gauge _SnapshotMetric_MinElapsedVersion_Gauge;
        readonly Gauge _SnapshotMetric_MaxSaveElapsedMs_Gauge;
        readonly Gauge _SnapshotMetric_AvgSaveElapsedMs_Gauge;
        readonly Gauge _SnapshotMetric_MinSaveElapsedMs_Gauge;
        public SnapshotMetricHandler()
        {
            _SnapshotMetric_SaveCount_Gauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.SaveCount)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            _SnapshotMetric_MaxElapsedVersion_Gauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MaxElapsedVersion)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            _SnapshotMetric_AvgElapsedVersion_Gauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.AvgElapsedVersion)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            _SnapshotMetric_MinElapsedVersion_Gauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MinElapsedVersion)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            _SnapshotMetric_MaxSaveElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MaxSaveElapsedMs)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            _SnapshotMetric_AvgSaveElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.AvgSaveElapsedMs)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
            _SnapshotMetric_MinSaveElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(SnapshotMetric)}_{nameof(SnapshotMetric.MinSaveElapsedMs)}", string.Empty, nameof(SnapshotMetric.Actor), nameof(SnapshotMetric.Snapshot));
        }
        public void Handle(List<SnapshotMetric> snapshotMetrics)
        {
            foreach (var item in snapshotMetrics)
            {
                _SnapshotMetric_SaveCount_Gauge.WithLabels(item.Actor, item.Snapshot).Set(item.SaveCount, item.Timestamp);
                _SnapshotMetric_MaxElapsedVersion_Gauge.WithLabels(item.Actor, item.Snapshot).Set(item.MaxElapsedVersion, item.Timestamp);
                _SnapshotMetric_AvgElapsedVersion_Gauge.WithLabels(item.Actor, item.Snapshot).Set(item.AvgElapsedVersion, item.Timestamp);
                _SnapshotMetric_MinElapsedVersion_Gauge.WithLabels(item.Actor, item.Snapshot).Set(item.MinElapsedVersion, item.Timestamp);
                _SnapshotMetric_MaxSaveElapsedMs_Gauge.WithLabels(item.Actor, item.Snapshot).Set(item.MaxSaveElapsedMs, item.Timestamp);
                _SnapshotMetric_AvgSaveElapsedMs_Gauge.WithLabels(item.Actor, item.Snapshot).Set(item.AvgSaveElapsedMs, item.Timestamp);
                _SnapshotMetric_MinSaveElapsedMs_Gauge.WithLabels(item.Actor, item.Snapshot).Set(item.MinSaveElapsedMs, item.Timestamp);
            }
        }
    }
}
