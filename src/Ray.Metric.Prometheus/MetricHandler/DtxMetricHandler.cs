using Prometheus.Client;
using Ray.Metric.Core.Element;
using System.Collections.Generic;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class DtxMetricHandler
    {
        readonly Gauge _DtxMetric_MaxElapsedMs_Gauge;
        readonly Gauge _DtxMetric_AvgElapsedMs_Gauge;
        readonly Gauge _DtxMetric_MinElapsedMs_Gauge;
        readonly Gauge _DtxMetric_Times_Gauge;
        readonly Gauge _DtxMetric_Commits_Gauge;
        readonly Gauge _DtxMetric_Rollbacks_Gauge;
        public DtxMetricHandler()
        {
            _DtxMetric_MaxElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.MaxElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            _DtxMetric_AvgElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.AvgElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            _DtxMetric_MinElapsedMs_Gauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.MinElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            _DtxMetric_Times_Gauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Times)}", string.Empty, nameof(DtxMetric.Actor));
            _DtxMetric_Commits_Gauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Commits)}", string.Empty, nameof(DtxMetric.Actor));
            _DtxMetric_Rollbacks_Gauge = Metrics.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Rollbacks)}", string.Empty, nameof(DtxMetric.Actor));
        }
        public void Handle(List<DtxMetric> DtxMetrics)
        {
            foreach (var item in DtxMetrics)
            {
                _DtxMetric_MaxElapsedMs_Gauge.WithLabels(item.Actor).Set(item.MaxElapsedMs, item.Timestamp);
                _DtxMetric_AvgElapsedMs_Gauge.WithLabels(item.Actor).Set(item.AvgElapsedMs, item.Timestamp);
                _DtxMetric_MinElapsedMs_Gauge.WithLabels(item.Actor).Set(item.MinElapsedMs, item.Timestamp);
                _DtxMetric_Times_Gauge.WithLabels(item.Actor).Set(item.Times, item.Timestamp);
                _DtxMetric_Commits_Gauge.WithLabels(item.Actor).Set(item.Commits, item.Timestamp);
                _DtxMetric_Rollbacks_Gauge.WithLabels(item.Actor).Set(item.Rollbacks, item.Timestamp);
            }
        }
    }
}
