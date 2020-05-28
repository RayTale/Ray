using System.Collections.Generic;

namespace Ray.Core.Abstractions.Monitor
{
    public interface IMetricMonitor
    {
        void Report(EventMetricElement element);
        void Report(List<EventMetricElement> elements);
        void Report(FollowMetricElement element);

        void Report(List<FollowMetricElement> elements);
        void Report(SnapshotMetricElement element);
        void Report(DtxMetricElement element);
    }
}
