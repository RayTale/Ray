using Orleans;
using Ray.Core.Abstractions.Monitor;
using Ray.Core.Abstractions.Monitor.Actors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Ray.Core.Monitor
{
    public class EventMonitor : IEventMonitor
    {
        readonly Subject<EventMetricElement> eventSubject = new Subject<EventMetricElement>();
        readonly Subject<FollowEventMetricElement> followSubject = new Subject<FollowEventMetricElement>();
        public EventMonitor(IGrainFactory grainFactory)
        {
            var monitorActor = grainFactory.GetGrain<IMonitorActor>(0);
            eventSubject.Buffer(TimeSpan.FromSeconds(1)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var eventMetrics = new List<EventMetric>();
                var linkMetrics = new List<EventLinkMetricElement>();
                var actorMetrics = new List<ActorMetric>();
                foreach (var group in list.GroupBy(e => e.Actor))
                {
                    var actorGroup = group.GroupBy(g => g.ActorId).ToList();
                    actorMetrics.Add(new ActorMetric
                    {
                        Actor = group.Key,
                        Lives = actorGroup.Count(),
                        Events = group.Count(),
                        Ignores = group.Where(g => g.Ignore).Count(),
                        MaxEventsPerActor = actorGroup.Max(ag => ag.Count()),
                        AvgEventsPerActor = (int)actorGroup.Average(ag => ag.Count()),
                        MinEventsPerActor = actorGroup.Min(ag => ag.Count())
                    });
                    foreach (var evtGroup in group.GroupBy(e => e.Event))
                    {
                        var actorIdGroup = evtGroup.GroupBy(g => g.ActorId).ToList();
                        eventMetrics.Add(new EventMetric
                        {
                            Event = evtGroup.Key,
                            Actor = group.Key,
                            Events = evtGroup.Count(),
                            AvgInsertElapsedMs = (int)evtGroup.Average(e => e.InsertElapsedMs),
                            MaxInsertElapsedMs = evtGroup.Max(e => e.InsertElapsedMs),
                            MinInsertElapsedMs = evtGroup.Min(e => e.InsertElapsedMs),
                            MaxPerActor = actorIdGroup.Max(ag => ag.Count()),
                            AvgPerActor = (int)actorIdGroup.Average(ag => ag.Count()),
                            MinPerActor = actorIdGroup.Min(ag => ag.Count()),
                            Ignores = evtGroup.Where(g => g.Ignore).Count()
                        });

                        foreach (var fromEvtActorGroup in evtGroup.GroupBy(e => e.FromEventActor))
                        {
                            foreach (var fromEvtGroup in fromEvtActorGroup.GroupBy(e => e.FromEvent))
                            {
                                linkMetrics.Add(new EventLinkMetricElement
                                {
                                    Event = evtGroup.Key,
                                    FromEventActor = fromEvtActorGroup.Key,
                                    FromEvent = fromEvtGroup.Key,
                                    Actor = group.Key,
                                    Events = fromEvtGroup.Count(),
                                    Ignores = fromEvtGroup.Where(g => g.Ignore).Count(),
                                    MaxElapsedMs = fromEvtGroup.Max(fg => fg.IntervalPrevious),
                                    AvgElapsedMs = (int)fromEvtGroup.Average(fg => fg.IntervalPrevious),
                                    MinElapsedMs = fromEvtGroup.Min(fg => fg.IntervalPrevious)
                                });
                            }
                        }
                    }
                }
                await monitorActor.Report(eventMetrics, actorMetrics, linkMetrics);
            });
            followSubject.Buffer(TimeSpan.FromSeconds(1)).Where(list => list.Count > 0).Subscribe(async list =>
            {
                var followActorMetrics = new List<FollowActorMetric>();
                var followEventMetrics = new List<FollowEventMetric>();
                foreach (var group in list.GroupBy(e => e.Actor))
                {
                    followActorMetrics.Add(new FollowActorMetric
                    {
                        Actor = group.Key,
                        FromActor = group.First().FromActor,
                        Events = group.Count(),
                        MaxElapsedMs = group.Max(g => g.ElapsedMs),
                        AvgElapsedMs = (int)group.Average(g => g.ElapsedMs),
                        MinElapsedMs = group.Min(g => g.ElapsedMs)
                    });
                    foreach (var evtgroup in group.GroupBy(e => e.Event))
                    {
                        followEventMetrics.Add(new FollowEventMetric
                        {
                            Actor = group.Key,
                            FromActor = group.First().FromActor,
                            Event = evtgroup.Key,
                            Events = evtgroup.Count(),
                            AvgElapsedMs = (int)evtgroup.Average(g => g.ElapsedMs),
                            MaxElapsedMs = evtgroup.Max(g => g.ElapsedMs),
                            MinElapsedMs = evtgroup.Min(g => g.ElapsedMs)
                        });
                    }
                }
                await monitorActor.Report(followActorMetrics, followEventMetrics);
            });
        }
        public void Report(EventMetricElement element)
        {
            eventSubject.OnNext(element);
        }

        public void Report(List<EventMetricElement> elements)
        {
            elements.ForEach(e => eventSubject.OnNext(e));
        }
        public void Report(FollowEventMetricElement element)
        {
            followSubject.OnNext(element);
        }

        public void Report(List<FollowEventMetricElement> elements)
        {
            elements.ForEach(e => followSubject.OnNext(e));
        }
    }
}
