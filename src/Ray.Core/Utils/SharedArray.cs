using System;
using System.Buffers;

namespace Ray.Core.Utils
{
    public class SharedArray : IDisposable
    {
        public SharedArray(byte[] bytes)
        {
            Buffer = bytes;
        }
        public byte[] Buffer { get; }
        public static SharedArray Rent(int minimumLength)
        {
            return new SharedArray(ArrayPool<byte>.Shared.Rent(minimumLength));
        }
        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(Buffer);
        }
    }
}
