using System.Threading.Tasks;
using Ray.Core.Abstractions;

namespace Ray.Core.Internal
{
    public class EventWithTask<K>
    {
        public EventWithTask(IEventBase<K> value, string uniqueId = null)
        {
            TaskSource = new TaskCompletionSource<bool>();
            Value = value;
            UniqueId = uniqueId;
        }
        public TaskCompletionSource<bool> TaskSource { get; set; }
        public IEventBase<K> Value { get; set; }
        public string UniqueId { get; set; }
    }
}
