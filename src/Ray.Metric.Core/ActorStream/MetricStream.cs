using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams;
using Ray.Metric.Core.Element;

namespace Ray.Metric.Core
{
    public class MetricStream : IMetricStream
    {
        private readonly IAsyncStream<EventMetric> EventMetricStream;
        private readonly IAsyncStream<ActorMetric> ActorMetricStream;
        private readonly IAsyncStream<EventSummaryMetric> EventSummaryMetricStream;
        private readonly IAsyncStream<EventLinkMetric> EventLinkMetricStream;
        private readonly IAsyncStream<FollowActorMetric> FollowActorMetricStream;
        private readonly IAsyncStream<FollowEventMetric> FollowEventMetricStream;
        private readonly IAsyncStream<FollowGroupMetric> FollowGroupMetricStream;
        private readonly IAsyncStream<SnapshotMetric> SnapshotMetricStream;
        private readonly IAsyncStream<SnapshotSummaryMetric> SnapshotSummaryMetricStream;
        private readonly IAsyncStream<DtxMetric> DtxMetricStream;
        private readonly IAsyncStream<DtxSummaryMetric> DtxSummaryMetricStream;

        public MetricStream(IStreamProvider streamProvider)
        {
            this.Id = new MetricStreamId
            {
                ProviderName = streamProvider.Name,
                StreamId = Guid.NewGuid(),
                Namespace = "RAY_METRIC"
            };
            this.EventMetricStream = streamProvider.GetStream<EventMetric>(this.Id.StreamId, this.Id.Namespace);
            this.ActorMetricStream = streamProvider.GetStream<ActorMetric>(this.Id.StreamId, this.Id.Namespace);
            this.EventSummaryMetricStream = streamProvider.GetStream<EventSummaryMetric>(this.Id.StreamId, this.Id.Namespace);
            this.EventLinkMetricStream = streamProvider.GetStream<EventLinkMetric>(this.Id.StreamId, this.Id.Namespace);
            this.FollowActorMetricStream = streamProvider.GetStream<FollowActorMetric>(this.Id.StreamId, this.Id.Namespace);
            this.FollowEventMetricStream = streamProvider.GetStream<FollowEventMetric>(this.Id.StreamId, this.Id.Namespace);
            this.FollowGroupMetricStream = streamProvider.GetStream<FollowGroupMetric>(this.Id.StreamId, this.Id.Namespace);
            this.SnapshotMetricStream = streamProvider.GetStream<SnapshotMetric>(this.Id.StreamId, this.Id.Namespace);
            this.SnapshotSummaryMetricStream = streamProvider.GetStream<SnapshotSummaryMetric>(this.Id.StreamId, this.Id.Namespace);
            this.DtxMetricStream = streamProvider.GetStream<DtxMetric>(this.Id.StreamId, this.Id.Namespace);
            this.DtxSummaryMetricStream = streamProvider.GetStream<DtxSummaryMetric>(this.Id.StreamId, this.Id.Namespace);
        }

        public MetricStreamId Id { get; }

        public Task OnNext(List<EventMetric> eventMetrics)
        {
            return this.EventMetricStream.OnNextBatchAsync(eventMetrics);
        }

        public Task OnNext(List<ActorMetric> actorMetrics)
        {
            return this.ActorMetricStream.OnNextBatchAsync(actorMetrics);
        }

        public Task OnNext(EventSummaryMetric summaryMetric)
        {
            return this.EventSummaryMetricStream.OnNextAsync(summaryMetric);
        }

        public Task OnNext(List<EventLinkMetric> eventLinkMetrics)
        {
            return this.EventLinkMetricStream.OnNextBatchAsync(eventLinkMetrics);
        }

        public Task OnNext(List<FollowActorMetric> actorMetrics)
        {
            return this.FollowActorMetricStream.OnNextBatchAsync(actorMetrics);
        }

        public Task OnNext(List<FollowEventMetric> followEventMetrics)
        {
            return this.FollowEventMetricStream.OnNextBatchAsync(followEventMetrics);
        }

        public Task OnNext(List<FollowGroupMetric> followGroupMetrics)
        {
            return this.FollowGroupMetricStream.OnNextBatchAsync(followGroupMetrics);
        }

        public Task OnNext(List<SnapshotMetric> snapshotMetrics)
        {
            return this.SnapshotMetricStream.OnNextBatchAsync(snapshotMetrics);
        }

        public Task OnNext(SnapshotSummaryMetric snapshotSummaryMetric)
        {
            return this.SnapshotSummaryMetricStream.OnNextAsync(snapshotSummaryMetric);
        }

        public Task OnNext(List<DtxMetric> dtxMetrics)
        {
            return this.DtxMetricStream.OnNextBatchAsync(dtxMetrics);
        }

        public Task OnNext(DtxSummaryMetric dtxSummaryMetric)
        {
            return this.DtxSummaryMetricStream.OnNextAsync(dtxSummaryMetric);
        }
    }
}
