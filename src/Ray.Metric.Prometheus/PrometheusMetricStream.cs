using Ray.Metric.Core;
using Ray.Metric.Core.Element;
using Ray.Metric.Prometheus.MetricHandler;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Metric.Prometheus
{
    public class PrometheusMetricStream : IMetricStream
    {
        readonly EventMetricHandler eventMetricHandler;
        readonly ActorMetricHandler actorMetricHandler;
        readonly EventSummaryMetricHandler eventSummaryMetric;
        readonly EventLinkMetricHandler eventLinkMetricHandler;
        readonly FollowActorMetricHandler followActorMetricHandler;
        readonly FollowFollowEventMetricHandler followFollowEventMetricHandler;
        readonly FollowGroupMetricHandler followGroupMetricHandler;
        readonly SnapshotMetricHandler snapshotMetricHandler;
        readonly SnapshotSummaryMetricHandler snapshotSummaryMetricHandler;
        readonly DtxMetricHandler dtxMetricHandler;
        readonly DtxSummaryMetricHandler dtxSummaryMetricHandler;

        public PrometheusMetricStream(
            EventMetricHandler eventMetricHandler,
            ActorMetricHandler actorMetricHandler,
            EventSummaryMetricHandler eventSummaryMetric,
            EventLinkMetricHandler eventLinkMetricHandler,
            FollowActorMetricHandler followActorMetricHandler,
            FollowFollowEventMetricHandler followFollowEventMetricHandler,
            FollowGroupMetricHandler followGroupMetricHandler,
            SnapshotMetricHandler snapshotMetricHandler,
            SnapshotSummaryMetricHandler snapshotSummaryMetricHandler,
            DtxMetricHandler dtxMetricHandler,
            DtxSummaryMetricHandler dtxSummaryMetricHandler)
        {
            this.eventMetricHandler = eventMetricHandler;
            this.actorMetricHandler = actorMetricHandler;
            this.eventSummaryMetric = eventSummaryMetric;
            this.eventLinkMetricHandler = eventLinkMetricHandler;
            this.followActorMetricHandler = followActorMetricHandler;
            this.followFollowEventMetricHandler = followFollowEventMetricHandler;
            this.followGroupMetricHandler = followGroupMetricHandler;
            this.snapshotMetricHandler = snapshotMetricHandler;
            this.snapshotSummaryMetricHandler = snapshotSummaryMetricHandler;
            this.dtxMetricHandler = dtxMetricHandler;
            this.dtxSummaryMetricHandler = dtxSummaryMetricHandler;
        }
        public Task OnNext(List<EventMetric> eventMetrics)
        {
            eventMetricHandler.Handle(eventMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<ActorMetric> actorMetrics)
        {
            actorMetricHandler.Handle(actorMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(EventSummaryMetric summaryMetric)
        {
            eventSummaryMetric.Handle(summaryMetric);
            return Task.CompletedTask;
        }

        public Task OnNext(List<EventLinkMetric> eventLinkMetrics)
        {
            eventLinkMetricHandler.Handle(eventLinkMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<FollowActorMetric> followActorMetrics)
        {
            followActorMetricHandler.Handle(followActorMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<FollowEventMetric> followEventMetrics)
        {
            followFollowEventMetricHandler.Handle(followEventMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<FollowGroupMetric> followGroupMetrics)
        {
            followGroupMetricHandler.Handle(followGroupMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<SnapshotMetric> snapshotMetrics)
        {
            snapshotMetricHandler.Handle(snapshotMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(SnapshotSummaryMetric snapshotSummaryMetric)
        {
            snapshotSummaryMetricHandler.Handle(snapshotSummaryMetric);
            return Task.CompletedTask;
        }

        public Task OnNext(List<DtxMetric> dtxMetrics)
        {
            dtxMetricHandler.Handle(dtxMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(DtxSummaryMetric dtxSummaryMetric)
        {
            dtxSummaryMetricHandler.Handle(dtxSummaryMetric);
            return Task.CompletedTask;
        }
    }
}
