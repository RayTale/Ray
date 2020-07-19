using System;
using System.Text;
using Ray.Core.Utils;

namespace Ray.Core.Serialization
{
    public struct TransportMessage
    {
        public TransportMessage(string name, byte[] bytes)
        {
            this.Name = name;
            this.Bytes = bytes;
        }

        public string Name { get; set; }

        public byte[] Bytes { get; set; }

        public SharedArray ConvertToBytes()
        {
            var typeBytes = Encoding.UTF8.GetBytes(this.Name);
            var array = SharedArray.Rent(typeBytes.Length + 1 + this.Bytes.Length + sizeof(ushort));
            var span = array.AsSpan();
            span[0] = (byte)HeaderType.Common;
            BitConverter.TryWriteBytes(span.Slice(1), (ushort)typeBytes.Length);
            typeBytes.CopyTo(span.Slice(1 + sizeof(ushort)));
            this.Bytes.CopyTo(span.Slice(1 + sizeof(ushort) + typeBytes.Length));
            return array;
        }

        public static bool TryParse(byte[] bytes, out TransportMessage wrapper)
        {
            if (bytes[0] == (byte)HeaderType.Common)
            {
                var bytesSpan = bytes.AsSpan();
                var eventTypeLength = BitConverter.ToUInt16(bytesSpan.Slice(1, sizeof(ushort)));
                wrapper = new TransportMessage
                {
                    Name = Encoding.UTF8.GetString(bytesSpan.Slice(sizeof(ushort) + 1, eventTypeLength)),
                    Bytes = bytesSpan.Slice(sizeof(ushort) + 1 + eventTypeLength).ToArray()
                };
                return true;
            }

            wrapper = default;
            return false;
        }
    }
}
