using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Metric.Core;
using Ray.Metric.Core.Element;
using Ray.Metric.Prometheus.MetricHandler;

namespace Ray.Metric.Prometheus
{
    public class PrometheusMetricStream : IMetricStream
    {
        private readonly EventMetricHandler eventMetricHandler;
        private readonly ActorMetricHandler actorMetricHandler;
        private readonly EventSummaryMetricHandler eventSummaryMetric;
        private readonly EventLinkMetricHandler eventLinkMetricHandler;
        private readonly FollowActorMetricHandler followActorMetricHandler;
        private readonly FollowFollowEventMetricHandler followFollowEventMetricHandler;
        private readonly FollowGroupMetricHandler followGroupMetricHandler;
        private readonly SnapshotMetricHandler snapshotMetricHandler;
        private readonly SnapshotSummaryMetricHandler snapshotSummaryMetricHandler;
        private readonly DtxMetricHandler dtxMetricHandler;
        private readonly DtxSummaryMetricHandler dtxSummaryMetricHandler;

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
            this.eventMetricHandler.Handle(eventMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<ActorMetric> actorMetrics)
        {
            this.actorMetricHandler.Handle(actorMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(EventSummaryMetric summaryMetric)
        {
            this.eventSummaryMetric.Handle(summaryMetric);
            return Task.CompletedTask;
        }

        public Task OnNext(List<EventLinkMetric> eventLinkMetrics)
        {
            this.eventLinkMetricHandler.Handle(eventLinkMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<FollowActorMetric> followActorMetrics)
        {
            this.followActorMetricHandler.Handle(followActorMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<FollowEventMetric> followEventMetrics)
        {
            this.followFollowEventMetricHandler.Handle(followEventMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<FollowGroupMetric> followGroupMetrics)
        {
            this.followGroupMetricHandler.Handle(followGroupMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(List<SnapshotMetric> snapshotMetrics)
        {
            this.snapshotMetricHandler.Handle(snapshotMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(SnapshotSummaryMetric snapshotSummaryMetric)
        {
            this.snapshotSummaryMetricHandler.Handle(snapshotSummaryMetric);
            return Task.CompletedTask;
        }

        public Task OnNext(List<DtxMetric> dtxMetrics)
        {
            this.dtxMetricHandler.Handle(dtxMetrics);
            return Task.CompletedTask;
        }

        public Task OnNext(DtxSummaryMetric dtxSummaryMetric)
        {
            this.dtxSummaryMetricHandler.Handle(dtxSummaryMetric);
            return Task.CompletedTask;
        }
    }
}
