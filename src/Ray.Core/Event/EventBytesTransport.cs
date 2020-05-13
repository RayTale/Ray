using System;
using System.Buffers;
using System.Text;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Utils;

namespace Ray.Core.Event
{
    public readonly struct EventBytesTransport
    {
        public EventBytesTransport(string eventCode, object grainId, byte[] baseBytes, byte[] eventBytes)
        {
            EventTypeCode = eventCode;
            GrainId = grainId;
            BaseBytes = baseBytes;
            EventBytes = eventBytes;
        }
        /// <summary>
        /// 事件TypeCode
        /// </summary>
        public string EventTypeCode { get; }
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
        public SharedArray GetBytes()
        {
            var eventTypeBytes = Encoding.UTF8.GetBytes(EventTypeCode);
            byte[] actorIdBytes;
            if (GrainId is long id)
            {
                actorIdBytes = BitConverter.GetBytes(id);
            }
            else if (GrainId is string strId)
            {
                actorIdBytes = Encoding.UTF8.GetBytes(strId);
            }
            else
            {
                throw new PrimaryKeyTypeException(EventTypeCode);
            }
            var buffer= SharedArray.Rent(1+sizeof(ushort)*3+sizeof(int)+eventTypeBytes.Length+actorIdBytes.Length+BaseBytes.Length+EventBytes.Length);
            var bufferSpan = buffer.Buffer.AsSpan();
            bufferSpan[0] = (byte)TransportType.Event;
            BitConverter.TryWriteBytes(bufferSpan.Slice(1), (ushort)eventTypeBytes.Length);
            BitConverter.TryWriteBytes(bufferSpan.Slice(1+sizeof(ushort)), (ushort)actorIdBytes.Length);
            BitConverter.TryWriteBytes(bufferSpan.Slice(1 + sizeof(ushort)*2), (ushort)BaseBytes.Length);
            BitConverter.TryWriteBytes(bufferSpan.Slice(1 + sizeof(ushort) * 3), EventBytes.Length);
            var bodySlice = bufferSpan.Slice(1 + sizeof(ushort) * 3 + sizeof(int));
            eventTypeBytes.CopyTo(bodySlice);
            var actorIdSlice = bodySlice.Slice(eventTypeBytes.Length);
            actorIdBytes.CopyTo(actorIdSlice);
            var baseSlice = actorIdSlice.Slice(actorIdBytes.Length);
            BaseBytes.CopyTo(baseSlice);
            EventBytes.CopyTo(baseSlice.Slice(BaseBytes.Length));
            return buffer;
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
                    var id = Encoding.UTF8.GetString(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
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
                    Encoding.UTF8.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
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
                        Encoding.UTF8.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        actorId,
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    ));
                }
                else
                {
                    var actorId = Encoding.UTF8.GetString(bytesSpan.Slice(skipLength + eventTypeLength, actorIdBytesLength));
                    return (true, new EventBytesTransport(
                        Encoding.UTF8.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
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
