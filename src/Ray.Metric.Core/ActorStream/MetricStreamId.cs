using System;

namespace Ray.Metric.Core
{
    public struct MetricStreamId
    {
        public string ProviderName { get; set; }
        public Guid StreamId { get; set; }
        public string Namespace { get; set; }
    }
}
