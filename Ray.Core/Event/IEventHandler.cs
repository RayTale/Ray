namespace Ray.Core.Event
{
    public interface IEventHandler<S>
    {
        void Apply(S state, IEvent evt);
    }
}
