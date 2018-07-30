namespace Ray.Core.EventSourcing
{
    public interface IEventHandle<S>
    {
        void Apply(S state, IEvent evt);
    }
}
