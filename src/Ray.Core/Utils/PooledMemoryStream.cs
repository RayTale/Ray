using System;
using System.IO;
using System.Buffers;

namespace Ray.Core.Utils
{
    public class PooledMemoryStream : Stream
    {
        /// <summary>create writable memory stream with default parameters</summary>
        /// <remarks>buffer is allocated from ArrayPool.Shared</remarks>
        public PooledMemoryStream()
            : this(ArrayPool<byte>.Shared)
        {
        }

        /// <summary>create writable memory stream with specified ArrayPool</summary>
        /// <remarks>buffer is allocated from ArrayPool</remarks>
        public PooledMemoryStream(ArrayPool<byte> pool)
            : this(pool, 4096)
        {
        }

        /// <summary>create writable memory stream with ensuring buffer length</summary>
        /// <remarks>buffer is allocated from ArrayPool</remarks>
        public PooledMemoryStream(ArrayPool<byte> pool, int capacity)
        {
            this._mPool = pool;
            this.currentbuffer = this._mPool.Rent(capacity);
            this.length = 0;
            this.canWrite = true;
            this.position = 0;
        }

        /// <summary>create readonly MemoryStream without buffer copy</summary>
        /// <remarks>data will be read from 'data' parameter</summary>
        public PooledMemoryStream(byte[] data)
        {
            this._mPool = null;
            this.currentbuffer = data;
            this.length = data.Length;
            this.canWrite = false;
        }

        public override bool CanRead
        {
            get
            {
                return true;
            }
        }

        public override bool CanSeek => true;

        public override bool CanWrite => this.canWrite;

        public override long Length => this.length;

        public override long Position
        {
            get => this.position;
            set
            {
                this.position = value;
            }
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int readlen = count > (int)(this.length - this.position) ? (int)(this.length - this.position) : count;
            if (readlen > 0)
            {
                Buffer.BlockCopy(this.currentbuffer, (int)this.position, buffer, offset, readlen);
                this.position += readlen;
                return readlen;
            }
            else
            {
                return 0;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long oldValue = this.position;
            switch ((int)origin)
            {
                case (int)SeekOrigin.Begin:
                    this.position = offset;
                    break;
                case (int)SeekOrigin.End:
                    this.position = this.length - offset;
                    break;
                case (int)SeekOrigin.Current:
                    this.position += offset;
                    break;
                default:
                    throw new InvalidOperationException("unknown SeekOrigin");
            }

            if (this.position < 0 || this.position > this.length)
            {
                this.position = oldValue;
                throw new IndexOutOfRangeException();
            }

            return this.position;
        }

        private void ReallocateBuffer(int minimumRequired)
        {
            var tmp = this._mPool.Rent(minimumRequired);
            Buffer.BlockCopy(this.currentbuffer, 0, tmp, 0, this.currentbuffer.Length);
            this._mPool.Return(this.currentbuffer);
            this.currentbuffer = tmp;
        }

        public override void SetLength(long value)
        {
            if (!this.canWrite)
            {
                throw new NotSupportedException("stream is readonly");
            }

            if (value > int.MaxValue)
            {
                throw new IndexOutOfRangeException("overflow");
            }

            if (value < 0)
            {
                throw new IndexOutOfRangeException("underflow");
            }

            this.length = value;
            if (this.currentbuffer.Length < this.length)
            {
                this.ReallocateBuffer((int)this.length);
            }
        }

        /// <summary>
        /// write data to stream
        /// </summary>
        /// <remarks>if stream data length is over int.MaxValue, this method throws IndexOutOfRangeException</remarks>
        /// <param name="buffer">buffer</param>
        /// <param name="offset">offset</param>
        /// <param name="count">count</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!this.canWrite)
            {
                throw new InvalidOperationException("stream is readonly");
            }

            long endOffset = this.position + count;
            if (endOffset > this.currentbuffer.Length)
            {
                this.ReallocateBuffer((int)(endOffset) * 2);
            }

            Buffer.BlockCopy(buffer, offset, this.currentbuffer, (int)this.position, count);
            if (endOffset > this.length)
            {
                this.length = endOffset;
            }

            this.position = endOffset;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (this._mPool != null && this.currentbuffer != null)
            {
                this._mPool.Return(this.currentbuffer);
                this.currentbuffer = null;
            }
        }

        /// <summary>
        /// ensure the buffer size
        /// </summary>
        /// <remarks>
        /// capacity != stream buffer length
        /// </remarks>
        /// <param name="capacity">size to reserve</param>
        public void Reserve(int capacity)
        {
            if (capacity > this.currentbuffer.Length)
            {
                this.ReallocateBuffer(capacity);
            }
        }

        /// <summary>
        /// Create newly allocated buffer and copy the stream data
        /// </summary>
        /// <returns>A new buffer</returns>
        public byte[] ToArray()
        {
            var ret = new byte[this.length];
            Buffer.BlockCopy(this.currentbuffer, 0, ret, 0, (int)this.length);
            return ret;
        }

        /// <summary>Create ArraySegment for current stream data without allocation buffer</summary>
        /// <remarks>After disposing stream, manipulating returned value(read or write) may cause undefined behavior</remarks>
        /// <returns>Returns a segment for current stream.</returns>
        public ArraySegment<byte> ToUnsafeArraySegment()
        {
            return new ArraySegment<byte>(this.currentbuffer, 0, (int)this.length);
        }

        /// <summary>
        /// Converts a buffer to readonly memory
        /// </summary>
        /// <returns>Readonly Memory is returned.</returns>
        public ReadOnlyMemory<byte> ToReadOnlyMemory()
        {
            return new ReadOnlyMemory<byte>(this.currentbuffer, 0, (int)this.length);
        }

        private readonly ArrayPool<byte> _mPool;
        private byte[] currentbuffer;
        private readonly bool canWrite;
        private long length;
        private long position;
    }
}