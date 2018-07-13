using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public class EventTransactionWrap<K>
    {
        public static EventTransactionWrap<K> Create(IEventBase<K> value, string uniqueId = null)
        {
            return new EventTransactionWrap<K>
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
    public class EventBytesTransactionWrap<K> : EventTransactionWrap<K>
    {
        public static EventBytesTransactionWrap<K> Create(IEventBase<K> value, byte[] bytes, string uniqueId = null)
        {
            return new EventBytesTransactionWrap<K>
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
