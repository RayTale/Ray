using Orleans.Concurrency;
using Ray.Core.Utils;
using System;

namespace Ray.Core.Event
{
    [Immutable]
    public class EventBase
    {
        public EventBase() { }
        public EventBase(long version, long timestamp)
        {
            Version = version;
            Timestamp = timestamp;
        }
        public long Version { get; set; }
        public long Timestamp { get; set; }
        public SharedArray ConvertToBytes()
        {
            var memory = SharedArray.Rent(sizeof(long) * 2);
            var span = memory.AsSpan();
            BitConverter.TryWriteBytes(span, Version);
            BitConverter.TryWriteBytes(span.Slice(sizeof(long)), Timestamp);
            return memory;
        }
        public static EventBase Parse(Span<byte> bytes)
        {
            return new EventBase(BitConverter.ToInt64(bytes), BitConverter.ToInt64(bytes.Slice(sizeof(long))));
        }
    }
}
