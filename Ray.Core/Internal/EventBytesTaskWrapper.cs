using System.Threading.Tasks;

namespace Ray.Core.Internal
{
    public class EventBytesTaskWrapper<K> : EventTaskWrapper<K>
    {
        public static EventBytesTaskWrapper<K> Create(IEventBase<K> value, byte[] bytes, string uniqueId = null)
        {
            return new EventBytesTaskWrapper<K>
            {
                TaskSource = new TaskCompletionSource<bool>(),
                Value = value,
                Bytes = bytes,
                UniqueId = uniqueId
            };
        }
        public byte[] Bytes { get; set; }
        public bool Result { get; set; }
    }
}
