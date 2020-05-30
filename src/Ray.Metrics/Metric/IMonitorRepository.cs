using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Metrics.Metric
{
    public interface IMonitorRepository
    {
        Task Insert(List<EventMetric> eventMetrics);
        Task Insert(List<ActorMetric> actorMetrics);
        Task Insert(EventSummaryMetric summaryMetric);
        Task Insert(List<EventLinkMetric> eventLinkMetrics);
        Task Insert(List<FollowActorMetric> actorMetrics);
        Task Insert(List<FollowEventMetric> followEventMetrics);
        Task Insert(List<FollowGroupMetric> followGroupMetrics);
        Task Insert(List<SnapshotMetric> snapshotMetrics);
        Task Insert(SnapshotSummaryMetric snapshotSummaryMetric);
        Task Insert(List<DtxMetric> dtxMetrics);
        Task Insert(DtxSummaryMetric dtxSummaryMetric);
    }
}
