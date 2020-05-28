using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.Abstractions.Monitor;
using Ray.Core.Abstractions.Monitor.Actors;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;

namespace Ray.Core.Monitor.Actors
{
    [Orleans.Concurrency.Reentrant]
    public class MonitorActor : Grain, IMonitorActor
    {
        readonly IMonitorRepository monitorRepository;
        readonly Subject<EventMetric> eventSubject = new Subject<EventMetric>();
        readonly Subject<ActorMetric> actorSubject = new Subject<ActorMetric>();
        readonly Subject<EventLinkMetricElement> eventLinkSubject = new Subject<EventLinkMetricElement>();
        readonly Subject<FollowActorMetric> followActorSubject = new Subject<FollowActorMetric>();
        readonly Subject<FollowEventMetric> followEventSubject = new Subject<FollowEventMetric>();
        readonly Subject<FollowGroupMetric> followGroupSubject = new Subject<FollowGroupMetric>();
        readonly Subject<SnapshotMetric> snapshotSubject = new Subject<SnapshotMetric>();
        readonly Subject<DtxMetric> dtxSubject = new Subject<DtxMetric>();
        readonly ConcurrentDictionary<string, ConcurrentDictionary<string, List<EventLink>>> eventLinkDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, List<EventLink>>>();
        public MonitorActor(IMonitorRepository monitorRepository, IOptions<MonitorOptions> options)
        {
            this.monitorRepository = monitorRepository;
            eventSubject.Buffer(TimeSpan.FromSeconds(options.Value.EventMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
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
                await monitorRepository.Insert(eventMetrics);
            });
            actorSubject.Buffer(TimeSpan.FromSeconds(options.Value.ActorMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
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
                await monitorRepository.Insert(summaryMetric);
                await monitorRepository.Insert(actorMetrics);
            });
            eventLinkSubject.Buffer(TimeSpan.FromSeconds(options.Value.EventLinkMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var eventLinkMetrics = new List<EventLinkMetric>();
                foreach (var actorGroup in list.GroupBy(e => e.Actor))
                {
                    var actorLinkDict = eventLinkDict.GetOrAdd(actorGroup.Key, key => new ConcurrentDictionary<string, List<EventLink>>());
                    foreach (var evtGroup in actorGroup.GroupBy(e => e.Event))
                    {
                        foreach (var fromActorGroup in evtGroup.GroupBy(e => e.FromEventActor))
                        {
                            foreach (var fromEvtGroup in fromActorGroup.GroupBy(e => e.FromEvent))
                            {
                                var eventLinkList = actorLinkDict.GetOrAdd(evtGroup.Key, key => new List<EventLink>());
                                if (!eventLinkList.Exists(o => o.ParentActor == fromActorGroup.Key && o.ParentEvent == fromEvtGroup.Key))
                                {
                                    eventLinkList.Add(new EventLink
                                    {
                                        Actor = actorGroup.Key,
                                        Event = evtGroup.Key,
                                        ParentActor = fromActorGroup.Key,
                                        ParentEvent = fromEvtGroup.Key
                                    });
                                }
                                eventLinkMetrics.Add(new EventLinkMetric
                                {
                                    Actor = actorGroup.Key,
                                    Event = evtGroup.Key,
                                    ParentActor = fromActorGroup.Key,
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
                await monitorRepository.Insert(eventLinkMetrics);
            });
            followActorSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowActorMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
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
                await monitorRepository.Insert(followActorMetrics);
            });
            followEventSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowEventMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
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
                await monitorRepository.Insert(followEventMetrics);
            });
            snapshotSubject.Buffer(TimeSpan.FromSeconds(options.Value.SnapshotMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
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
                await monitorRepository.Insert(snapshotMetrics);
                await monitorRepository.Insert(summaryMetric);
            });
            dtxSubject.Buffer(TimeSpan.FromSeconds(options.Value.DtxMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
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
                await monitorRepository.Insert(dtxMetrics);
                await monitorRepository.Insert(summaryMetric);
            });
            followGroupSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowGroupMetricFrequency)).Where(list => list.Count > 0).Subscribe(async list =>
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
                await monitorRepository.Insert(followGroupMetrics);
            });
        }
        public Task Report(List<EventMetric> eventMetrics, List<ActorMetric> actorMetrics, List<EventLinkMetricElement> eventLinkMetrics)
        {
            eventMetrics.ForEach(e => eventSubject.OnNext(e));
            actorMetrics.ForEach(e => actorSubject.OnNext(e));
            eventLinkMetrics.ForEach(e => eventLinkSubject.OnNext(e));
            return Task.CompletedTask;
        }

        public Task Report(List<FollowActorMetric> followActorMetrics, List<FollowEventMetric> followEventMetrics, List<FollowGroupMetric> followGroupMetrics)
        {
            followActorMetrics.ForEach(e => followActorSubject.OnNext(e));
            followEventMetrics.ForEach(e => followEventSubject.OnNext(e));
            followGroupMetrics.ForEach(e => followGroupSubject.OnNext(e));
            return Task.CompletedTask;
        }

        public Task Report(List<SnapshotMetric> snapshotMetrics)
        {
            snapshotMetrics.ForEach(e => snapshotSubject.OnNext(e));
            return Task.CompletedTask;
        }

        public Task Report(List<DtxMetric> snapshotMetrics)
        {
            snapshotMetrics.ForEach(e => dtxSubject.OnNext(e));
            return Task.CompletedTask;
        }
    }
}
