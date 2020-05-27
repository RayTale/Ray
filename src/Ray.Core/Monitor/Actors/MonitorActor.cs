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
    public class MonitorActor : Grain, IMonitorActor
    {
        readonly IMonitorRepository monitorRepository;
        readonly Subject<EventMetric> eventSubject = new Subject<EventMetric>();
        readonly Subject<ActorMetric> actorSubject = new Subject<ActorMetric>();
        readonly Subject<EventLinkMetricElement> eventLinkSubject = new Subject<EventLinkMetricElement>();
        readonly Subject<FollowActorMetric> followActorSubject = new Subject<FollowActorMetric>();
        readonly Subject<FollowEventMetric> followEventSubject = new Subject<FollowEventMetric>();
        readonly ConcurrentDictionary<string, ConcurrentDictionary<string, EventLink>> eventLinkDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, EventLink>>();
        public MonitorActor(IMonitorRepository monitorRepository, IOptions<MonitorOptions> options)
        {
            this.monitorRepository = monitorRepository;
            eventSubject.Buffer(TimeSpan.FromSeconds(options.Value.EventMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
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
                //TODO 存储
            });
            actorSubject.Buffer(TimeSpan.FromSeconds(options.Value.ActorMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var actorMetrics = new List<ActorMetric>();
                var summaryMetric = new SummaryMetric
                {
                    Events = list.Sum(e => e.Events),
                    Ignores = list.Sum(e => e.Ignores),
                    ActorLives = list.Sum(e => e.Lives),
                    AvgEventsPerActor = (int)list.Average(e => e.AvgEventsPerActor),
                    MaxEventsPerActor = list.Max(e => e.MaxEventsPerActor),
                    MinEventsPerActor = list.Min(e => e.MinEventsPerActor),
                    Timestamp = timestamp
                };
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
                //TODO 存储
            });
            eventLinkSubject.Buffer(TimeSpan.FromSeconds(options.Value.EventLinkMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var eventLinkMetrics = new List<EventLinkMetric>();
                foreach (var evtGroup in list.GroupBy(e => e.Event))
                {
                    var linkDict = eventLinkDict.GetOrAdd(evtGroup.Key, key => new ConcurrentDictionary<string, EventLink>());
                    foreach (var actorGroup in evtGroup.GroupBy(e => e.Actor))
                    {

                        foreach (var fromEvtActorGroup in evtGroup.GroupBy(e => e.FromEventActor))
                        {
                            foreach (var fromEvtGroup in evtGroup.GroupBy(e => e.FromEvent))
                            {

                            }
                        }
                    }
                }
                //TODO 存储
            });
            followActorSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowActorMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
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
                //TODO 存储
            });
            followEventSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowEventMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
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
                //TODO 存储
            });
        }
        public Task Report(List<EventMetric> eventMetrics, List<ActorMetric> actorMetrics, List<EventLinkMetricElement> eventLinkMetrics)
        {
            eventMetrics.ForEach(e => eventSubject.OnNext(e));
            actorMetrics.ForEach(e => actorSubject.OnNext(e));
            eventLinkMetrics.ForEach(e => eventLinkSubject.OnNext(e));
            return Task.CompletedTask;
        }

        public Task Report(List<FollowActorMetric> followActorMetrics, List<FollowEventMetric> followEventMetrics)
        {
            followActorMetrics.ForEach(e => followActorSubject.OnNext(e));
            followEventMetrics.ForEach(e => followEventSubject.OnNext(e));
            return Task.CompletedTask;
        }
    }
}
