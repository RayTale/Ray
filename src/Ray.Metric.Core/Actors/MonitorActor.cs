﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Metric.Core.Element;
using Ray.Metric.Core.Options;

namespace Ray.Metric.Core.Actors
{
    [Orleans.Concurrency.Reentrant]
    public class MonitorActor : Grain, IMonitorActor
    {
        private readonly Subject<EventMetric> eventSubject = new Subject<EventMetric>();
        private readonly Subject<ActorMetric> actorSubject = new Subject<ActorMetric>();
        private readonly Subject<EventLinkMetric> eventLinkSubject = new Subject<EventLinkMetric>();
        private readonly Subject<FollowActorMetric> followActorSubject = new Subject<FollowActorMetric>();
        private readonly Subject<FollowEventMetric> followEventSubject = new Subject<FollowEventMetric>();
        private readonly Subject<FollowGroupMetric> followGroupSubject = new Subject<FollowGroupMetric>();
        private readonly Subject<SnapshotMetric> snapshotSubject = new Subject<SnapshotMetric>();
        private readonly Subject<DtxMetric> dtxSubject = new Subject<DtxMetric>();

        public MonitorActor(IOptions<MonitorOptions> options, IMetricStream metricStream)
        {
            this.eventSubject.Buffer(TimeSpan.FromSeconds(options.Value.EventMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var eventMetrics = new List<EventMetric>();
                foreach (var actorGroup in list.GroupBy(e => e.Actor))
                {
                    foreach (var evtGroup in actorGroup.GroupBy(e => e.Event))
                    {
                        eventMetrics.Add(new EventMetric
                        {
                            Actor = actorGroup.Key,
                            Event = evtGroup.Key,
                            Events = evtGroup.Sum(e => e.Events),
                            Ignores = evtGroup.Sum(e => e.Ignores),
                            AvgPerActor = (int)evtGroup.Average(e => e.AvgPerActor),
                            MaxPerActor = evtGroup.Max(e => e.MaxPerActor),
                            MinPerActor = evtGroup.Min(e => e.MinPerActor),
                            AvgInsertElapsedMs = (int)evtGroup.Average(e => e.AvgInsertElapsedMs),
                            MaxInsertElapsedMs = evtGroup.Max(e => e.MaxInsertElapsedMs),
                            MinInsertElapsedMs = evtGroup.Min(e => e.MinInsertElapsedMs),
                            Timestamp = timestamp
                        });
                    }
                }

                await metricStream.OnNext(eventMetrics);
            });
            this.actorSubject.Buffer(TimeSpan.FromSeconds(options.Value.ActorMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var actorMetrics = new List<ActorMetric>();
                foreach (var actorGroup in list.GroupBy(e => e.Actor))
                {
                    actorMetrics.Add(new ActorMetric
                    {
                        Actor = actorGroup.Key,
                        Events = actorGroup.Sum(e => e.Events),
                        Ignores = actorGroup.Sum(e => e.Ignores),
                        Lives = actorGroup.Sum(e => e.Lives),
                        AvgEventsPerActor = (int)actorGroup.Average(e => e.AvgEventsPerActor),
                        MaxEventsPerActor = actorGroup.Max(e => e.MaxEventsPerActor),
                        MinEventsPerActor = actorGroup.Min(e => e.MinEventsPerActor),
                        Timestamp = timestamp
                    });
                }

                var summaryMetric = new EventSummaryMetric
                {
                    Events = actorMetrics.Sum(e => e.Events),
                    Ignores = actorMetrics.Sum(e => e.Ignores),
                    ActorLives = actorMetrics.Sum(e => e.Lives),
                    AvgEventsPerActor = (int)actorMetrics.Average(e => e.AvgEventsPerActor),
                    MaxEventsPerActor = actorMetrics.Max(e => e.MaxEventsPerActor),
                    MinEventsPerActor = actorMetrics.Min(e => e.MinEventsPerActor),
                    Timestamp = timestamp
                };
                await Task.WhenAll(metricStream.OnNext(summaryMetric), metricStream.OnNext(actorMetrics));
            });
            this.eventLinkSubject.Buffer(TimeSpan.FromSeconds(options.Value.EventLinkMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var eventLinkMetrics = new List<EventLinkMetric>();
                foreach (var actorGroup in list.GroupBy(e => e.Actor))
                {
                    foreach (var evtGroup in actorGroup.GroupBy(e => e.Event))
                    {
                        foreach (var parentActorGroup in evtGroup.GroupBy(e => e.ParentActor))
                        {
                            foreach (var fromEvtGroup in parentActorGroup.GroupBy(e => e.ParentEvent))
                            {
                                eventLinkMetrics.Add(new EventLinkMetric
                                {
                                    Actor = actorGroup.Key,
                                    Event = evtGroup.Key,
                                    ParentActor = parentActorGroup.Key,
                                    ParentEvent = fromEvtGroup.Key,
                                    Events = fromEvtGroup.Sum(e => e.Events),
                                    Ignores = fromEvtGroup.Sum(e => e.Ignores),
                                    AvgElapsedMs = (int)fromEvtGroup.Average(e => e.AvgElapsedMs),
                                    MaxElapsedMs = fromEvtGroup.Max(e => e.MaxElapsedMs),
                                    MinElapsedMs = fromEvtGroup.Min(e => e.MinElapsedMs),
                                    Timestamp = timestamp
                                });
                            }
                        }
                    }
                }

                await metricStream.OnNext(eventLinkMetrics);
            });
            this.followActorSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowActorMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var followActorMetrics = new List<FollowActorMetric>();
                foreach (var group in list.GroupBy(e => e.Actor))
                {
                    followActorMetrics.Add(new FollowActorMetric
                    {
                        Actor = group.Key,
                        FromActor = group.First().FromActor,
                        Events = group.Sum(e => e.Events),
                        AvgElapsedMs = (int)group.Average(e => e.AvgElapsedMs),
                        MaxElapsedMs = group.Max(e => e.MaxElapsedMs),
                        MinElapsedMs = group.Min(e => e.MinElapsedMs),
                        Timestamp = timestamp
                    });
                }

                await metricStream.OnNext(followActorMetrics);
            });
            this.followEventSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowEventMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var followEventMetrics = new List<FollowEventMetric>();
                foreach (var group in list.GroupBy(e => e.Actor))
                {
                    foreach (var evtGroup in group.GroupBy(e => e.Event))
                    {
                        followEventMetrics.Add(new FollowEventMetric
                        {
                            Actor = group.Key,
                            FromActor = group.First().FromActor,
                            Event = evtGroup.Key,
                            AvgElapsedMs = (int)evtGroup.Average(e => e.AvgElapsedMs),
                            MaxElapsedMs = evtGroup.Max(e => e.MaxElapsedMs),
                            MinElapsedMs = evtGroup.Min(e => e.MinElapsedMs),
                            Timestamp = timestamp
                        });
                    }
                }

                await metricStream.OnNext(followEventMetrics);
            });
            this.snapshotSubject.Buffer(TimeSpan.FromSeconds(options.Value.SnapshotMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var snapshotMetrics = new List<SnapshotMetric>();
                foreach (var group in list.GroupBy(e => e.Actor))
                {
                    snapshotMetrics.Add(new SnapshotMetric
                    {
                        Actor = group.Key,
                        Snapshot = group.First().Snapshot,
                        SaveCount = list.Sum(e => e.SaveCount),
                        AvgElapsedVersion = (int)list.Average(e => e.AvgElapsedVersion),
                        AvgSaveElapsedMs = (int)list.Average(e => e.AvgSaveElapsedMs),
                        MaxElapsedVersion = list.Max(e => e.MaxElapsedVersion),
                        MaxSaveElapsedMs = list.Max(e => e.MaxSaveElapsedMs),
                        MinElapsedVersion = list.Min(e => e.MinElapsedVersion),
                        MinSaveElapsedMs = list.Min(e => e.MinSaveElapsedMs),
                        Timestamp = timestamp
                    });
                }

                var summaryMetric = new SnapshotSummaryMetric
                {
                    SaveCount = snapshotMetrics.Sum(e => e.SaveCount),
                    AvgElapsedVersion = (int)snapshotMetrics.Average(e => e.AvgElapsedVersion),
                    AvgSaveElapsedMs = (int)snapshotMetrics.Average(e => e.AvgSaveElapsedMs),
                    MaxElapsedVersion = snapshotMetrics.Max(e => e.MaxElapsedVersion),
                    MaxSaveElapsedMs = snapshotMetrics.Max(e => e.MaxSaveElapsedMs),
                    MinElapsedVersion = snapshotMetrics.Min(e => e.MinElapsedVersion),
                    MinSaveElapsedMs = snapshotMetrics.Min(e => e.MinSaveElapsedMs),
                    Timestamp = timestamp
                };
                await Task.WhenAll(metricStream.OnNext(snapshotMetrics), metricStream.OnNext(summaryMetric));
            });
            this.dtxSubject.Buffer(TimeSpan.FromSeconds(options.Value.DtxMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var dtxMetrics = new List<DtxMetric>();
                foreach (var group in list.GroupBy(e => e.Actor))
                {
                    dtxMetrics.Add(new DtxMetric
                    {
                        Actor = group.Key,
                        Times = group.Sum(e => e.Times),
                        Commits = group.Sum(e => e.Commits),
                        Rollbacks = group.Sum(e => e.Rollbacks),
                        AvgElapsedMs = (int)group.Average(e => e.AvgElapsedMs),
                        MaxElapsedMs = group.Max(e => e.MaxElapsedMs),
                        MinElapsedMs = group.Min(e => e.MinElapsedMs),
                        Timestamp = timestamp
                    });
                }

                var summaryMetric = new DtxSummaryMetric
                {
                    Times = dtxMetrics.Sum(e => e.Times),
                    Commits = dtxMetrics.Sum(e => e.Commits),
                    Rollbacks = dtxMetrics.Sum(e => e.Rollbacks),
                    AvgElapsedMs = (int)dtxMetrics.Average(e => e.AvgElapsedMs),
                    MaxElapsedMs = dtxMetrics.Max(e => e.MaxElapsedMs),
                    MinElapsedMs = dtxMetrics.Min(e => e.MinElapsedMs),
                    Timestamp = timestamp
                };
                await Task.WhenAll(metricStream.OnNext(dtxMetrics), metricStream.OnNext(summaryMetric));
            });
            this.followGroupSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowGroupMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var followGroupMetrics = new List<FollowGroupMetric>();
                foreach (var group in list.GroupBy(e => e.Group))
                {
                    followGroupMetrics.Add(new FollowGroupMetric
                    {
                        Group = group.Key,
                        Events = group.Sum(e => e.Events),
                        AvgElapsedMs = (int)group.Average(e => e.AvgElapsedMs),
                        MaxElapsedMs = group.Max(e => e.MaxElapsedMs),
                        MinElapsedMs = group.Min(e => e.MinElapsedMs),
                        Timestamp = timestamp
                    });
                }

                await metricStream.OnNext(followGroupMetrics);
            });
        }

        public Task Report(List<EventMetric> eventMetrics, List<ActorMetric> actorMetrics, List<EventLinkMetric> eventLinkMetrics)
        {
            eventMetrics.ForEach(e => this.eventSubject.OnNext(e));
            actorMetrics.ForEach(e => this.actorSubject.OnNext(e));
            eventLinkMetrics.ForEach(e => this.eventLinkSubject.OnNext(e));
            return Task.CompletedTask;
        }

        public Task Report(List<FollowActorMetric> followActorMetrics, List<FollowEventMetric> followEventMetrics, List<FollowGroupMetric> followGroupMetrics)
        {
            followActorMetrics.ForEach(e => this.followActorSubject.OnNext(e));
            followEventMetrics.ForEach(e => this.followEventSubject.OnNext(e));
            followGroupMetrics.ForEach(e => this.followGroupSubject.OnNext(e));
            return Task.CompletedTask;
        }

        public Task Report(List<SnapshotMetric> snapshotMetrics)
        {
            snapshotMetrics.ForEach(e => this.snapshotSubject.OnNext(e));
            return Task.CompletedTask;
        }

        public Task Report(List<DtxMetric> snapshotMetrics)
        {
            snapshotMetrics.ForEach(e => this.dtxSubject.OnNext(e));
            return Task.CompletedTask;
        }
    }
}
