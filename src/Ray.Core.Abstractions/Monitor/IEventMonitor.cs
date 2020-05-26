using System.Collections.Generic;

namespace Ray.Core.Abstractions.Monitor
{
    public interface IEventMonitor
    {
        void Report(EventMetricElement element);
        void Report(List<EventMetricElement> elements);
        void Report(FollowEventMetricElement element);

        void Report(List<FollowEventMetricElement> elements);
    }
}
