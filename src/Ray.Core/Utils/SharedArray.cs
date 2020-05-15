using System;
using System.Buffers;

namespace Ray.Core.Utils
{
    public class SharedArray : IDisposable
    {
        readonly byte[] _Buffer;
        public SharedArray(int length)
        {
            _Buffer = ArrayPool<byte>.Shared.Rent(length / 4096 + 1);
            Length = length;
        }
        public int Length { get; }
        public Span<byte> AsSpan() => _Buffer.AsSpan().Slice(0, Length);
        public static SharedArray Rent(int length)
        {
            return new SharedArray(length);
        }
        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_Buffer);
        }
    }
}
