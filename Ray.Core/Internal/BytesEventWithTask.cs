using Ray.Core.Abstractions;

namespace Ray.Core.Internal
{
    public class BytesEventWithTask<K> : EventWithTask<K>
    {
        public BytesEventWithTask(IEventBase<K> value, byte[] bytes, string uniqueId = null) : base(value, uniqueId)
        {
            Bytes = bytes;
        }
        public byte[] Bytes { get; set; }
        public bool Result { get; set; }
    }
}
