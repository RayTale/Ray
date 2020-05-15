using System;
using System.Text;
using Ray.Core.Utils;

namespace Ray.Core.Serialization
{
    public struct CommonTransport
    {
        public CommonTransport(string typeFullName, byte[] bytes)
        {
            TypeFullName = typeFullName;
            Bytes = bytes;
        }
        public string TypeFullName { get; set; }
        public byte[] Bytes { get; set; }
        public SharedArray ConvertToBytes()
        {
            var typeBytes = Encoding.UTF8.GetBytes(TypeFullName);
            var array = SharedArray.Rent(typeBytes.Length + 1 + Bytes.Length + sizeof(ushort));
            var span = array.AsSpan();
            span[0] = (byte)TransportType.Common;
            BitConverter.TryWriteBytes(span.Slice(1), (ushort)typeBytes.Length);
            typeBytes.CopyTo(span.Slice(1 + sizeof(ushort)));
            Bytes.CopyTo(span.Slice(1 + sizeof(ushort) + typeBytes.Length));
            return array;
        }
        public static bool TryParse(byte[] bytes, out CommonTransport wrapper)
        {
            if (bytes[0] == (byte)TransportType.Common)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                wrapper = new CommonTransport
                {
                    TypeFullName = Encoding.UTF8.GetString(bytesSpan.Slice(sizeof(ushort) + 1, eventTypeLength)),
                    Bytes = bytesSpan.Slice(sizeof(ushort) + 1 + eventTypeLength).ToArray()
                };
                return true;
            }
            wrapper = default;
            return false;
        }
    }
}
