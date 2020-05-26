using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.Abstractions.Monitor;
using Ray.Core.Abstractions.Monitor.Actors;
using System;
using System.Collections.Generic;
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
        public MonitorActor(IMonitorRepository monitorRepository, IOptions<MonitorOptions> options)
        {
            this.monitorRepository = monitorRepository;
            eventSubject.Buffer(TimeSpan.FromSeconds(options.Value.EventMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
            {

            });
            actorSubject.Buffer(TimeSpan.FromSeconds(options.Value.ActorMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
            {

            });
            eventLinkSubject.Buffer(TimeSpan.FromSeconds(options.Value.EventLinkMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
            {

            });
            followActorSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowActorMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
            {

            });
            followEventSubject.Buffer(TimeSpan.FromSeconds(options.Value.FollowEventMetricFrequency)).Where(list => list.Count > 0).Subscribe(list =>
            {

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
