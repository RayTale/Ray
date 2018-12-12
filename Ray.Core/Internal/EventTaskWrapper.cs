using System.Threading.Tasks;

namespace Ray.Core.Internal
{
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
