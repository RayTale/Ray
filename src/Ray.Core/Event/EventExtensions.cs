using System;
using Ray.Core.Utils;

namespace Ray.Core.Event
{
    public static class EventExtensions
    {
        public static SharedArray ConvertToBytes(this EventBasicInfo eventBase)
        {
            var memory = SharedArray.Rent(sizeof(long) * 2);
            var span = memory.AsSpan();
            BitConverter.TryWriteBytes(span, eventBase.Version);
            BitConverter.TryWriteBytes(span.Slice(sizeof(long)), eventBase.Timestamp);
            return memory;
        }

        public static EventBasicInfo ParseToEventBase(this Span<byte> bytes)
        {
            return new EventBasicInfo(BitConverter.ToInt64(bytes), BitConverter.ToInt64(bytes.Slice(sizeof(long))));
        }
    }
}
