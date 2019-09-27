using System;
using Ray.Core.Utils;

namespace Ray.Core.Event
{
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
        public byte[] GetBytes()
        {
            using var ms = new PooledMemoryStream();
            ms.Write(BitConverter.GetBytes(Version));
            ms.Write(BitConverter.GetBytes(Timestamp));
            return ms.ToArray();
        }
        public static EventBase FromBytes(byte[] bytes)
        {
            var bytesSpan = bytes.AsSpan();
            return new EventBase(BitConverter.ToInt64(bytesSpan), BitConverter.ToInt64(bytesSpan.Slice(sizeof(long))));
        }
    }
}
