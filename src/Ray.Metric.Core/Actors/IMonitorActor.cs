using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Core.Actors
{
    public interface IMonitorActor : IGrainWithIntegerKey
    {
        Task Report(List<EventMetric> eventMetrics, List<ActorMetric> actorMetrics, List<EventLinkMetric> eventLinkMetrics);

        Task Report(List<FollowActorMetric> followActorMetrics, List<FollowEventMetric> followEventMetrics, List<FollowGroupMetric> followGroupMetrics);

        Task Report(List<SnapshotMetric> snapshotMetrics);

        Task Report(List<DtxMetric> snapshotMetrics);
    }
}
