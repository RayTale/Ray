using System;
using System.Text;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Utils;

namespace Ray.Core.Event
{
    public class BytesTransport
    {
        public string EventType { get; set; }
        public object ActorId { get; set; }
        public byte[] BaseBytes { get; set; }
        public byte[] EventBytes { get; set; }
        public byte[] GetBytes()
        {
            var eventTypeBytes = Encoding.Default.GetBytes(EventType);
            byte[] actorIdBytes;
            if (ActorId is long id)
            {
                actorIdBytes = BitConverter.GetBytes(id);
            }
            else if (ActorId is string strId)
            {
                actorIdBytes = Encoding.Default.GetBytes(strId);
            }
            else
            {
                throw new PrimaryKeyTypeException(EventType);
            }
            using (var ms = new PooledMemoryStream())
            {
                ms.WriteByte((byte)BytesType.Event);
                ms.Write(BitConverter.GetBytes((ushort)eventTypeBytes.Length));
                ms.Write(BitConverter.GetBytes((ushort)actorIdBytes.Length));
                ms.Write(BitConverter.GetBytes((ushort)BaseBytes.Length));
                ms.Write(BitConverter.GetBytes(EventBytes.Length));
                ms.Write(eventTypeBytes);
                ms.Write(actorIdBytes);
                ms.Write(BaseBytes);
                ms.Write(EventBytes);
                return ms.ToArray();
            }
        }
        public static (bool success, long actorId) GetActorIdWithLong(byte[] bytes)
        {
            if (bytes[0] == (byte)BytesType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                var result = BitConverter.ToInt64(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
                if (result is long value)
                    return (true, value);
            }
            return (false, default);
        }
        public static (bool success, string actorId) GetActorIdWithString(byte[] bytes)
        {
            if (bytes[0] == (byte)BytesType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                var result = Encoding.Default.GetString(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
                if (result is string value)
                    return (true, value);
            }
            return (false, default);
        }
        public static (bool success, BytesTransport transport) FromBytesWithNoId(byte[] bytes)
        {
            if (bytes[0] == (byte)BytesType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                var baseBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(2 * sizeof(ushort) + 1, sizeof(ushort)));
                var eventBytesLength = BitConverter.ToInt32(bytesSpan.Slice(3 * sizeof(ushort) + 1, sizeof(int)));
                var skipLength = 3 * sizeof(ushort) + 1 + sizeof(int);
                return (true, new BytesTransport
                {
                    EventType = Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                    BaseBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                    EventBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                });
            }
            return (false, default);
        }
        public static (bool success, BytesTransport transport) FromBytesWithLongId(byte[] bytes)
        {
            if (bytes[0] == (byte)BytesType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                var baseBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(2 * sizeof(ushort) + 1, sizeof(ushort)));
                var eventBytesLength = BitConverter.ToInt32(bytesSpan.Slice(3 * sizeof(ushort) + 1, sizeof(int)));
                var skipLength = 3 * sizeof(ushort) + 1 + sizeof(int);
                var result = BitConverter.ToInt64(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
                if (result is long actorId)
                {
                    return (true, new BytesTransport
                    {
                        EventType = Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        ActorId = actorId,
                        BaseBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        EventBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    });
                }
            }
            return (false, default);
        }
        public static (bool success, BytesTransport transport) FromBytesWithStringId(byte[] bytes)
        {
            if (bytes[0] == (byte)BytesType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                var baseBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(2 * sizeof(ushort) + 1, sizeof(ushort)));
                var eventBytesLength = BitConverter.ToInt32(bytesSpan.Slice(3 * sizeof(ushort) + 1, sizeof(int)));
                var skipLength = 3 * sizeof(ushort) + 1 + sizeof(int);
                var result = Encoding.Default.GetString(bytesSpan.Slice(skipLength + eventTypeLength, actorIdBytesLength));
                if (result is string actorId)
                {
                    return (true, new BytesTransport
                    {
                        EventType = Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        ActorId = actorId,
                        BaseBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        EventBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    });
                }
            }
            return (false, default);
        }
    }
}
