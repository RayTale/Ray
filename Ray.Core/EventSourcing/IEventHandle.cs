namespace Ray.Core.EventSourcing
{
    public interface IEventHandle
    {
        void Apply(object state, IEvent evt);
    }
}
