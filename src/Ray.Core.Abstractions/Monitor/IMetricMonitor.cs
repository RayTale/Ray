using System.Collections.Generic;

namespace Ray.Core.Abstractions.Monitor
{
    public interface IMetricMonitor
    {
        void Report(EventMetricElement element);
        void Report(List<EventMetricElement> elements);
        void Report(FollowEventMetricElement element);

        void Report(List<FollowEventMetricElement> elements);
        void Report(SnapshotMetricElement element);
        void Report(DtxMetricElement element);
    }
}
