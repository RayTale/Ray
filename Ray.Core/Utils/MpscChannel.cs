using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Ray.Core.Utils
{
    public class ChannelContainer<K, T, R>
    {
        readonly ConcurrentDictionary<K, MpscChannel<T, R>> insertChannelDict = new ConcurrentDictionary<K, MpscChannel<T, R>>();
        readonly Func<BufferBlock<DataTaskWrap<T, R>>, Task> Process;
        public ChannelContainer(Func<BufferBlock<DataTaskWrap<T, R>>, Task> process)
        {
            Process = process;
        }
        public MpscChannel<T, R> GetChannel(K key)
        {
            return insertChannelDict.GetOrAdd(key, k =>
            {
                return new MpscChannel<T, R>(Process);
            });
        }
    }
    /// <summary>
    /// multi producter single consumer channel
    /// </summary>
    /// <typeparam name="T">data type produced by producer</typeparam>
    /// <typeparam name="R">data type returned after processing</typeparam>
    public class MpscChannel<T, R>
    {
        readonly BufferBlock<DataTaskWrap<T, R>> buffer = new BufferBlock<DataTaskWrap<T, R>>();
        readonly Func<BufferBlock<DataTaskWrap<T, R>>, Task> handler;
        public MpscChannel(Func<BufferBlock<DataTaskWrap<T, R>>, Task> handler)
        {
            this.handler = handler;
        }
        public async Task<R> WriteAsync(T data)
        {
            var wrap = new DataTaskWrap<T, R>(data);
            if (!buffer.Post(wrap))
                await buffer.SendAsync(wrap);
            if (consuming == 0)
                ThreadPool.QueueUserWorkItem(StartConsumer);
            return await wrap.TaskSource.Task;
        }
        int consuming = 0;
        private async void StartConsumer(object state)
        {
            if (Interlocked.CompareExchange(ref consuming, 1, 0) == 0)
            {
                try
                {
                    while (await buffer.OutputAvailableAsync())
                    {
                        await handler(buffer);
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref consuming, 0);
                }
            }
        }
        public void Complete()
        {
            buffer.Complete();
        }
    }
    public class DataTaskWrap<T, R>
    {
        public DataTaskWrap(T data)
        {
            Value = data;
        }
        public TaskCompletionSource<R> TaskSource { get; set; } = new TaskCompletionSource<R>();
        public T Value { get; set; }
    }
}
