namespace Ray.Core.EventSourcing
{
    public class EventInfo<T>
    {
        public bool IsComplete
        {
            get;
            set;
        }
        public IEventBase<T> Event
        {
            get;
            set;
        }
    }
}
