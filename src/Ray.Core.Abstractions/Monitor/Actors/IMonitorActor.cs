using Orleans;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.Abstractions.Monitor.Actors
{
    public interface IMonitorActor : IGrainWithIntegerKey
    {
        Task Report(List<EventMetric> eventMetrics, List<ActorMetric> actorMetrics, List<EventLinkMetricElement> eventLinkMetrics);
        Task Report(List<FollowActorMetric> followActorMetrics, List<FollowEventMetric> followEventMetrics);
        Task Report(List<SnapshotMetric> snapshotMetrics);
        Task Report(List<DtxMetric> snapshotMetrics);
    }
}
