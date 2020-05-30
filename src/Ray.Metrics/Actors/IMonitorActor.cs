using Orleans;
using Ray.Metrics.Metric;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Metrics.Actors
{
    public interface IMonitorActor : IGrainWithIntegerKey
    {
        Task Report(List<EventMetric> eventMetrics, List<ActorMetric> actorMetrics, List<EventLinkMetric> eventLinkMetrics);
        Task Report(List<FollowActorMetric> followActorMetrics, List<FollowEventMetric> followEventMetrics, List<FollowGroupMetric> followGroupMetrics);
        Task Report(List<SnapshotMetric> snapshotMetrics);
        Task Report(List<DtxMetric> snapshotMetrics);
    }
}
