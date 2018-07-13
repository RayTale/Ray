using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public class EventFlowWrap<K>
    {
        public static EventFlowWrap<K> Create(IEventBase<K> value, string uniqueId = null)
        {
            return new EventFlowWrap<K>
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
    public class EventBytesFlowWrap<K> : EventFlowWrap<K>
    {
        public static EventBytesFlowWrap<K> Create(IEventBase<K> value, byte[] bytes, string uniqueId = null)
        {
            return new EventBytesFlowWrap<K>
            {
                TaskSource = new TaskCompletionSource<bool>(),
                Value = value,
                Bytes = bytes,
                UniqueId = uniqueId
            };
        }
        public byte[] Bytes { get; set; }
    }
}
