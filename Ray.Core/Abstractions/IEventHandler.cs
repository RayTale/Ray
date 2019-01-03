namespace Ray.Core.Abstractions
{
    public interface IEventHandler<S>
    {
        void Apply(S state, IEvent evt);
    }
}
