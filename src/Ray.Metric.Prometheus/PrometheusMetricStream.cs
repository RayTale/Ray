using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Prometheus.Client;
using Ray.Metric.Core;
using Ray.Metric.Core.Metric;

namespace Ray.Metric.Prometheus
{
    public class PrometheusMetricStream : IMetricStream
    {
        readonly Gauge _Event_Count_Gauge;
        readonly Gauge _Event_MaxPerActor_Gauge;
        public PrometheusMetricStream()
        {
            _Event_Count_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.Events)}", "number of events per event", nameof(EventMetric.Actor), nameof(EventMetric.Event));
            _Event_MaxPerActor_Gauge = Metrics.CreateGauge($"{nameof(EventMetric)}_{nameof(EventMetric.Events)}", "max number of events per actor", nameof(EventMetric.Actor), nameof(EventMetric.Event));
        }
        public Task OnNext(List<EventMetric> eventMetrics)
        {
            foreach (var item in eventMetrics)
            {
                var _Event_Count_Gauge_LabelMetric = _Event_Count_Gauge.WithLabels(item.Actor, item.Event);
                _Event_Count_Gauge_LabelMetric.Set(item.Events, item.Timestamp);
                var _Event_MaxPerActor_Gauge_LabelMetric = _Event_MaxPerActor_Gauge.WithLabels(item.Actor, item.Event);
                _Event_MaxPerActor_Gauge_LabelMetric.Set(item.Events, item.Timestamp);
            }
            return Task.CompletedTask;
        }

        public Task OnNext(List<ActorMetric> actorMetrics)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(EventSummaryMetric summaryMetric)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(List<EventLinkMetric> eventLinkMetrics)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(List<FollowActorMetric> actorMetrics)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(List<FollowEventMetric> followEventMetrics)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(List<FollowGroupMetric> followGroupMetrics)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(List<SnapshotMetric> snapshotMetrics)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(SnapshotSummaryMetric snapshotSummaryMetric)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(List<DtxMetric> dtxMetrics)
        {
            throw new NotImplementedException();
        }

        public Task OnNext(DtxSummaryMetric dtxSummaryMetric)
        {
            throw new NotImplementedException();
        }
    }
}
