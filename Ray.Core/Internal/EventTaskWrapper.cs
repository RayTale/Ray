using System.Threading.Tasks;

namespace Ray.Core.Internal
{
    public class EventWrapper<K>
    {
        public EventWrapper(IEventBase<K> value, string uniqueId = null)
        {
            Value = value;
            UniqueId = uniqueId;
        }
        public IEventBase<K> Value { get; set; }
        public string UniqueId { get; set; }
    }
    public class EventTaskWrapper<K>
    {
        public static EventTaskWrapper<K> Create(IEventBase<K> value, string uniqueId = null)
        {
            return new EventTaskWrapper<K>
            {
                TaskSource = new TaskCompletionSource<bool>(),
                Value = value,
                UniqueId = uniqueId
            };
        }
        public TaskCompletionSource<bool> TaskSource { get; set; }
        public IEventBase<K> Value { get; set; }
        public string UniqueId { get; set; }
    }
}
