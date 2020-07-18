using System;
using System.Buffers;

namespace Ray.Core.Utils
{
    public class SharedArray : IDisposable
    {
        private readonly byte[] Buffer;

        public SharedArray(int length)
        {
            this.Buffer = ArrayPool<byte>.Shared.Rent((length / 4096 + 1) * 4096);
            this.Length = length;
        }

        public int Length { get; }

        public Span<byte> AsSpan() => this.Buffer.AsSpan().Slice(0, this.Length);

        public static SharedArray Rent(int length)
        {
            return new SharedArray(length);
        }

        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(this.Buffer);
        }
    }
}
