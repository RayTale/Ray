using System;
using System.Text;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Utils;

namespace Ray.Core.Event
{
    public class EventBytesTransport
    {
        /// <summary>
        /// 事件TypeFullName
        /// </summary>
        public string EventType { get; set; }
        /// <summary>
        /// 事件GrainId
        /// </summary>
        public object GrainId { get; set; }
        /// <summary>
        /// 事件base信息的bytes
        /// </summary>
        public byte[] BaseBytes { get; set; }
        /// <summary>
        /// 事件本身的bytes
        /// </summary>
        public byte[] EventBytes { get; set; }
        /// <summary>
        /// 全部bytes缓存信息
        /// </summary>
        private byte[] _bytes;
        public byte[] GetBytes()
        {
            if (_bytes == default)
            {
                var eventTypeBytes = Encoding.Default.GetBytes(EventType);
                byte[] actorIdBytes;
                if (GrainId is long id)
                {
                    actorIdBytes = BitConverter.GetBytes(id);
                }
                else if (GrainId is string strId)
                {
                    actorIdBytes = Encoding.Default.GetBytes(strId);
                }
                else
                {
                    throw new PrimaryKeyTypeException(EventType);
                }
                using (var ms = new PooledMemoryStream())
                {
                    ms.WriteByte((byte)TransportType.Event);
                    ms.Write(BitConverter.GetBytes((ushort)eventTypeBytes.Length));
                    ms.Write(BitConverter.GetBytes((ushort)actorIdBytes.Length));
                    ms.Write(BitConverter.GetBytes((ushort)BaseBytes.Length));
                    ms.Write(BitConverter.GetBytes(EventBytes.Length));
                    ms.Write(eventTypeBytes);
                    ms.Write(actorIdBytes);
                    ms.Write(BaseBytes);
                    ms.Write(EventBytes);
                    _bytes = ms.ToArray();
                }
            }
            return _bytes;
        }
        public static (bool success, long actorId) GetActorIdWithLong(byte[] bytes)
        {
            if (bytes[0] == (byte)TransportType.Event)
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
            if (bytes[0] == (byte)TransportType.Event)
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
        public static (bool success, EventBytesTransport transport) FromBytesWithNoId(byte[] bytes)
        {
            if (bytes[0] == (byte)TransportType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                var baseBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(2 * sizeof(ushort) + 1, sizeof(ushort)));
                var eventBytesLength = BitConverter.ToInt32(bytesSpan.Slice(3 * sizeof(ushort) + 1, sizeof(int)));
                var skipLength = 3 * sizeof(ushort) + 1 + sizeof(int);
                return (true, new EventBytesTransport
                {
                    EventType = Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                    BaseBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                    EventBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                });
            }
            return (false, default);
        }
        public static (bool success, EventBytesTransport transport) FromBytesWithLongId(byte[] bytes)
        {
            if (bytes[0] == (byte)TransportType.Event)
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
                    return (true, new EventBytesTransport
                    {
                        EventType = Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        GrainId = actorId,
                        BaseBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        EventBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    });
                }
            }
            return (false, default);
        }
        public static (bool success, EventBytesTransport transport) FromBytesWithStringId(byte[] bytes)
        {
            if (bytes[0] == (byte)TransportType.Event)
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
                    return (true, new EventBytesTransport
                    {
                        EventType = Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        GrainId = actorId,
                        BaseBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        EventBytes = bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    });
                }
            }
            return (false, default);
        }
    }
}
