using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Messaging;

namespace Ray.Core.Internal
{
    public abstract class ReplicaGrain<K, S, W> : FollowGrain<K, S, W>
        where S : class, IState<K>, new()
        where W : IBytesMessage
    {
        public ReplicaGrain(ILogger logger) : base(logger)
        {
        }
        protected IEventHandler<S> EventHandler { get; private set; }
        public override Task OnActivateAsync()
        {
            EventHandler = ServiceProvider.GetService<IEventHandler<S>>();
            return base.OnActivateAsync();
        }
        protected override bool SaveSnapshot => false;
        protected override ValueTask OnEventDelivered(IEventBase<K> @event)
        {
            EventHandler.Apply(State, @event);
            return new ValueTask();
        }
    }
}
