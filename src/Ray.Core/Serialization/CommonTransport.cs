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
        public byte[] GetBytes()
        {
            var eventTypeBytes = Encoding.UTF8.GetBytes(TypeFullName);
            using var ms = new PooledMemoryStream();
            ms.WriteByte((byte)TransportType.Common);
            ms.Write(BitConverter.GetBytes((ushort)eventTypeBytes.Length));
            ms.Write(Bytes);
            return ms.ToArray();
        }
        public static (bool success, CommonTransport wrapper) FromBytes(byte[] bytes)
        {
            if (bytes[0] == (byte)TransportType.Common)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                return (true, new CommonTransport
                {
                    TypeFullName = Encoding.UTF8.GetString(bytesSpan.Slice(sizeof(ushort) + 1, eventTypeLength)),
                    Bytes = bytesSpan.Slice(sizeof(ushort) + 1 + eventTypeLength).ToArray()
                });
            }
            return (false, default);
        }
    }
}
