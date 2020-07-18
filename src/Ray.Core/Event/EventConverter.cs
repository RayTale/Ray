using System;
using System.Buffers;
using System.Text;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using Ray.Core.Utils;

namespace Ray.Core.Event
{
    public readonly ref struct EventConverter
    {
        public EventConverter(string eventUniqueName, object grainId, Span<byte> baseBytes, Span<byte> eventBytes)
        {
            EventUniqueName = eventUniqueName;
            GrainId = grainId;
            BaseBytes = baseBytes;
            EventBytes = eventBytes;
        }
        /// <summary>
        /// 事件唯一名称
        /// </summary>
        public string EventUniqueName { get; }
        /// <summary>
        /// 事件GrainId
        /// </summary>
        public object GrainId { get; }
        /// <summary>
        /// 事件base信息的bytes
        /// </summary>
        public Span<byte> BaseBytes { get; }
        /// <summary>
        /// 事件本身的bytes
        /// </summary>
        public Span<byte> EventBytes { get; }
        public SharedArray ConvertToBytes()
        {
            var eventTypeBytes = Encoding.UTF8.GetBytes(EventUniqueName);
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
                throw new PrimaryKeyTypeException(EventUniqueName);
            }
            var buffer = SharedArray.Rent(1 + sizeof(ushort) * 3 + sizeof(int) + eventTypeBytes.Length + actorIdBytes.Length + BaseBytes.Length + EventBytes.Length);
            var bufferSpan = buffer.AsSpan();
            bufferSpan[0] = (byte)HeaderType.Event;
            BitConverter.TryWriteBytes(bufferSpan.Slice(1), (ushort)eventTypeBytes.Length);
            BitConverter.TryWriteBytes(bufferSpan.Slice(1 + sizeof(ushort)), (ushort)actorIdBytes.Length);
            BitConverter.TryWriteBytes(bufferSpan.Slice(1 + sizeof(ushort) * 2), (ushort)BaseBytes.Length);
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
        public static bool TryParseActorId<PrimaryKey>(byte[] bytes, out PrimaryKey primaryKey)
        {
            if (bytes[0] == (byte)HeaderType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                if (typeof(PrimaryKey) == typeof(long))
                {
                    var id = BitConverter.ToInt64(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
                    if (id is PrimaryKey actorId)
                    {
                        primaryKey = actorId;
                        return true;
                    }
                }
                else
                {
                    var id = Encoding.UTF8.GetString(bytesSpan.Slice(3 * sizeof(ushort) + 1 + sizeof(int) + eventTypeLength, actorIdBytesLength));
                    if (id is PrimaryKey actorId)
                    {
                        primaryKey = actorId;
                        return true;
                    }
                }
            }
            primaryKey = default;
            return false;
        }
        public static bool TryParseWithNoId(byte[] bytes, out EventConverter value)
        {
            if (bytes[0] == (byte)HeaderType.Event)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                var actorIdBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(1 + sizeof(ushort), sizeof(ushort)));
                var baseBytesLength = BitConverter.ToUInt16(bytesSpan.Slice(2 * sizeof(ushort) + 1, sizeof(ushort)));
                var eventBytesLength = BitConverter.ToInt32(bytesSpan.Slice(3 * sizeof(ushort) + 1, sizeof(int)));
                var skipLength = 3 * sizeof(ushort) + 1 + sizeof(int);
                var data = new EventConverter(
                    Encoding.UTF8.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                    null,
                    bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength),
                    bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength)
                    );
                value = data;
                return true;
            }
            value = default;
            return false;
        }
        public static bool TryParse<PrimaryKey>(byte[] bytes, out EventConverter value)
        {
            if (bytes[0] == (byte)HeaderType.Event)
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
                    value = new EventConverter(
                        Encoding.UTF8.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        actorId,
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    );
                    return true;
                }
                else
                {
                    var actorId = Encoding.UTF8.GetString(bytesSpan.Slice(skipLength + eventTypeLength, actorIdBytesLength));
                    value = new EventConverter(
                        Encoding.UTF8.GetString(bytesSpan.Slice(skipLength, eventTypeLength)),
                        actorId,
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength, baseBytesLength).ToArray(),
                        bytesSpan.Slice(skipLength + eventTypeLength + actorIdBytesLength + baseBytesLength, eventBytesLength).ToArray()
                    );
                    return true;
                }
            }
            value = default;
            return false;
        }
    }
}
