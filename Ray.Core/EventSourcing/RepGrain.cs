using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public abstract class RepGrain<K, S, W> : AsyncGrain<K, S, W>
        where S : class, IState<K>, new()
        where W : MessageWrapper
    {
        protected override bool SaveSnapshot => false;
        protected abstract IEventHandle EventHandle { get; }
        protected override Task OnEventDelivered(IEventBase<K> @event)
        {
            EventHandle.Apply(State, @event);
            return Task.CompletedTask;
        }
    }
}
