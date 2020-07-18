﻿using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Core
{
    public interface IMetricStream
    {
        Task OnNext(List<EventMetric> eventMetrics);

        Task OnNext(List<ActorMetric> actorMetrics);

        Task OnNext(EventSummaryMetric summaryMetric);

        Task OnNext(List<EventLinkMetric> eventLinkMetrics);

        Task OnNext(List<FollowActorMetric> followActorMetrics);

        Task OnNext(List<FollowEventMetric> followEventMetrics);

        Task OnNext(List<FollowGroupMetric> followGroupMetrics);

        Task OnNext(List<SnapshotMetric> snapshotMetrics);

        Task OnNext(SnapshotSummaryMetric snapshotSummaryMetric);

        Task OnNext(List<DtxMetric> dtxMetrics);

        Task OnNext(DtxSummaryMetric dtxSummaryMetric);
    }
}
