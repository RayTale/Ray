using Orleans.Streams;
using Ray.Metric.Core.Metric;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Metric.Core
{
    public class MetricStream : IMetricStream
    {
        readonly IAsyncStream<EventMetric> _EventMetricStream;
        readonly IAsyncStream<ActorMetric> _ActorMetricStream;
        readonly IAsyncStream<EventSummaryMetric> _EventSummaryMetricStream;
        readonly IAsyncStream<EventLinkMetric> _EventLinkMetricStream;
        readonly IAsyncStream<FollowActorMetric> _FollowActorMetricStream;
        readonly IAsyncStream<FollowEventMetric> _FollowEventMetricStream;
        readonly IAsyncStream<FollowGroupMetric> _FollowGroupMetricStream;
        readonly IAsyncStream<SnapshotMetric> _SnapshotMetricStream;
        readonly IAsyncStream<SnapshotSummaryMetric> _SnapshotSummaryMetricStream;
        readonly IAsyncStream<DtxMetric> _DtxMetricStream;
        readonly IAsyncStream<DtxSummaryMetric> _DtxSummaryMetricStream;
        public MetricStream(IStreamProvider streamProvider)
        {
            Id = new MetricStreamId
            {
                ProviderName = streamProvider.Name,
                StreamId = Guid.NewGuid(),
                Namespace = "RAY_METRIC"
            };
            _EventMetricStream = streamProvider.GetStream<EventMetric>(Id.StreamId, Id.Namespace);
            _ActorMetricStream = streamProvider.GetStream<ActorMetric>(Id.StreamId, Id.Namespace);
            _EventSummaryMetricStream = streamProvider.GetStream<EventSummaryMetric>(Id.StreamId, Id.Namespace);
            _EventLinkMetricStream = streamProvider.GetStream<EventLinkMetric>(Id.StreamId, Id.Namespace);
            _FollowActorMetricStream = streamProvider.GetStream<FollowActorMetric>(Id.StreamId, Id.Namespace);
            _FollowEventMetricStream = streamProvider.GetStream<FollowEventMetric>(Id.StreamId, Id.Namespace);
            _FollowGroupMetricStream = streamProvider.GetStream<FollowGroupMetric>(Id.StreamId, Id.Namespace);
            _SnapshotMetricStream = streamProvider.GetStream<SnapshotMetric>(Id.StreamId, Id.Namespace);
            _SnapshotSummaryMetricStream = streamProvider.GetStream<SnapshotSummaryMetric>(Id.StreamId, Id.Namespace);
            _DtxMetricStream = streamProvider.GetStream<DtxMetric>(Id.StreamId, Id.Namespace);
            _DtxSummaryMetricStream = streamProvider.GetStream<DtxSummaryMetric>(Id.StreamId, Id.Namespace);
        }
        public MetricStreamId Id { get; }

        public Task OnNext(List<EventMetric> eventMetrics)
        {
            return _EventMetricStream.OnNextBatchAsync(eventMetrics);
        }

        public Task OnNext(List<ActorMetric> actorMetrics)
        {
            return _ActorMetricStream.OnNextBatchAsync(actorMetrics);
        }

        public Task OnNext(EventSummaryMetric summaryMetric)
        {
            return _EventSummaryMetricStream.OnNextAsync(summaryMetric);
        }

        public Task OnNext(List<EventLinkMetric> eventLinkMetrics)
        {
            return _EventLinkMetricStream.OnNextBatchAsync(eventLinkMetrics);
        }

        public Task OnNext(List<FollowActorMetric> actorMetrics)
        {
            return _FollowActorMetricStream.OnNextBatchAsync(actorMetrics);
        }

        public Task OnNext(List<FollowEventMetric> followEventMetrics)
        {
            return _FollowEventMetricStream.OnNextBatchAsync(followEventMetrics);
        }

        public Task OnNext(List<FollowGroupMetric> followGroupMetrics)
        {
            return _FollowGroupMetricStream.OnNextBatchAsync(followGroupMetrics);
        }

        public Task OnNext(List<SnapshotMetric> snapshotMetrics)
        {
            return _SnapshotMetricStream.OnNextBatchAsync(snapshotMetrics);
        }

        public Task OnNext(SnapshotSummaryMetric snapshotSummaryMetric)
        {
            return _SnapshotSummaryMetricStream.OnNextAsync(snapshotSummaryMetric);
        }

        public Task OnNext(List<DtxMetric> dtxMetrics)
        {
            return _DtxMetricStream.OnNextBatchAsync(dtxMetrics);
        }

        public Task OnNext(DtxSummaryMetric dtxSummaryMetric)
        {
            return _DtxSummaryMetricStream.OnNextAsync(dtxSummaryMetric);
        }
    }
}
