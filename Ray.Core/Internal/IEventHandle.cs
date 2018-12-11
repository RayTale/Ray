namespace Ray.Core.Internal
{
    public interface IEventHandle<S>
    {
        void Apply(S state, IEvent evt);
    }
}
