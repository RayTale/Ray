using System;
using System.Text;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Utils;

namespace Ray.Core.Event
{
    public readonly struct EventBytesTransport
    {
        public EventBytesTransport(string eventType, object grainId, byte[] baseBytes, byte[] eventBytes)
        {
            EventType = eventType;
            GrainId = grainId;
            BaseBytes = baseBytes;
            EventBytes = eventBytes;
        }
        /// <summary>
        /// 事件TypeFullName
        /// </summary>
        public string EventType { get; }
        /// <summary>
        /// 事件GrainId
        /// </summary>
        public object GrainId { get; }
        /// <summary>
        /// 事件base信息的bytes
        /// </summary>
        public byte[] BaseBytes { get; }
        /// <summary>
        /// 事件本身的bytes
        /// </summary>
        public byte[] EventBytes { get; }
        public byte[] GetBytes()
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
                return ms.ToArray();
            }
        }
        public static (bool success, PrimaryKey actorId) GetActorId<PrimaryKey>(byte[] bytes)
        {
            if (bytes[0] == (byte)TransportType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                if (typeof(PrimaryKey) == typeof(long))
                {
                    var id = BitConverter.ToInt64(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
                    if (id is PrimaryKey actorId)
                        return (true, actorId);
                }
                else
                {
                    var id = Encoding.Default.GetString(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
                    if (id is PrimaryKey actorId)
                        return (true, actorId);
                }
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
                return (true, new EventBytesTransport(
                    Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                    null,
                    bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                    bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    ));
            }
            return (false, default);
        }
        public static (bool success, EventBytesTransport transport) FromBytes<PrimaryKey>(byte[] bytes)
        {
            if (bytes[0] == (byte)TransportType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                var baseBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(2 * sizeof(ushort) + 1, sizeof(ushort)));
                var eventBytesLength = BitConverter.ToInt32(bytesSpan.Slice(3 * sizeof(ushort) + 1, sizeof(int)));
                var skipLength = 3 * sizeof(ushort) + 1 + sizeof(int);
                if (typeof(PrimaryKey) == typeof(long))
                {
                    var actorId = BitConverter.ToInt64(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
                    return (true, new EventBytesTransport(
                        Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        actorId,
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    ));
                }
                else
                {
                    var actorId = Encoding.Default.GetString(bytesSpan.Slice(skipLength + eventTypeLength, actorIdBytesLength));
                    return (true, new EventBytesTransport(
                        Encoding.Default.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        actorId,
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    ));
                }
            }
            return (false, default);
        }
    }
}
