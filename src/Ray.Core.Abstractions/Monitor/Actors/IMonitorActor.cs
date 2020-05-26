using Orleans;
using Orleans.Concurrency;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.Abstractions.Monitor.Actors
{
    public interface IMonitorActor : IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task Report(List<EventMetric> eventMetrics, List<ActorMetric> actorMetrics, List<EventLinkMetricElement> eventLinkMetrics);
        [AlwaysInterleave]
        Task Report(List<FollowActorMetric> followActorMetrics, List<FollowEventMetric> followEventMetrics);
    }
}
