namespace Ray.Core.Internal
{
    public interface IEventHandler<S>
    {
        void Apply(S state, IEvent evt);
    }
}
