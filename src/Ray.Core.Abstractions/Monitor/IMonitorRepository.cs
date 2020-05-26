using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.Abstractions.Monitor
{
    public interface IMonitorRepository
    {
        Task Append(List<EventMetric> eventMetrics);
        Task Append(List<ActorMetric> actorMetrics);
        Task Append(SummaryMetric summaryMetric);
    }
}
