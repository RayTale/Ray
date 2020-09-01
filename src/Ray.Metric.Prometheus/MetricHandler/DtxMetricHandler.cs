using System;
using System.Collections.Generic;
using Prometheus.Client;
using Prometheus.Client.Abstractions;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Prometheus.MetricHandler
{
    public class DtxMetricHandler
    {
        private readonly IMetricFamily<IGauge, ValueTuple<string>> DtxMetricMaxElapsedMsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> DtxMetricAvgElapsedMsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> DtxMetricMinElapsedMsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> DtxMetricTimesGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> DtxMetricCommitsGauge;
        private readonly IMetricFamily<IGauge, ValueTuple<string>> DtxMetricRollbacksGauge;

        public DtxMetricHandler(IMetricFactory metricFactory)
        {
            this.DtxMetricMaxElapsedMsGauge = metricFactory.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.MaxElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricAvgElapsedMsGauge = metricFactory.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.AvgElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricMinElapsedMsGauge = metricFactory.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.MinElapsedMs)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricTimesGauge = metricFactory.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Times)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricCommitsGauge = metricFactory.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Commits)}", string.Empty, nameof(DtxMetric.Actor));
            this.DtxMetricRollbacksGauge = metricFactory.CreateGauge($"{nameof(DtxMetric)}_{nameof(DtxMetric.Rollbacks)}", string.Empty, nameof(DtxMetric.Actor));
        }

        public void Handle(List<DtxMetric> DtxMetrics)
        {
            foreach (var item in DtxMetrics)
            {
                this.DtxMetricMaxElapsedMsGauge.WithLabels(item.Actor).Set(item.MaxElapsedMs, item.Timestamp);
                this.DtxMetricAvgElapsedMsGauge.WithLabels(item.Actor).Set(item.AvgElapsedMs, item.Timestamp);
                this.DtxMetricMinElapsedMsGauge.WithLabels(item.Actor).Set(item.MinElapsedMs, item.Timestamp);
                this.DtxMetricTimesGauge.WithLabels(item.Actor).Set(item.Times, item.Timestamp);
                this.DtxMetricCommitsGauge.WithLabels(item.Actor).Set(item.Commits, item.Timestamp);
                this.DtxMetricRollbacksGauge.WithLabels(item.Actor).Set(item.Rollbacks, item.Timestamp);
            }
        }
    }
}
