using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Ray.Core.Utils
{
    public class ChannelContainer<K, T, R> : IDisposable
    {
        readonly ConcurrentDictionary<K, MpscChannel<T, R>> channelDict = new ConcurrentDictionary<K, MpscChannel<T, R>>();
        readonly Timer monitorTimer;
        public ChannelContainer()
        {
            monitorTimer = new Timer(Monitor, null, 10 * 1000, 10 * 1000);
        }

        public MpscChannel<T, R> Create(K key, Func<BufferBlock<DataTaskWrapper<T, R>>, Task> consumer)
        {
            return channelDict.GetOrAdd(key, k =>
            {
                var channel = new MpscChannel<T, R>(consumer);
                ThreadPool.QueueUserWorkItem(channel.StartConsumer);
                return channel;
            });
        }
        private void Monitor(object state)
        {
            var releasedList = new List<K>();
            foreach (var channel in channelDict)
            {
                if (channel.Value.IsComplete)
                    releasedList.Add(channel.Key);
                else if (!channel.Value.InConsuming)
                {
                    ThreadPool.QueueUserWorkItem(channel.Value.StartConsumer);
                }
            }
            foreach (var key in releasedList)
            {
                channelDict.TryRemove(key, out var _);
            }
        }
        public void Dispose()
        {
            monitorTimer.Dispose();
        }
    }
    /// <summary>
    /// multi producter single consumer channel
    /// </summary>
    /// <typeparam name="T">data type produced by producer</typeparam>
    /// <typeparam name="R">data type returned after processing</typeparam>
    public class MpscChannel<T, R>
    {
        readonly BufferBlock<DataTaskWrapper<T, R>> buffer = new BufferBlock<DataTaskWrapper<T, R>>();
        readonly Func<BufferBlock<DataTaskWrapper<T, R>>, Task> consumer;
        public MpscChannel(Func<BufferBlock<DataTaskWrapper<T, R>>, Task> consumer)
        {
            this.consumer = consumer;
        }

        public async Task<R> WriteAsync(T data)
        {
            var wrap = new DataTaskWrapper<T, R>(data);
            if (!buffer.Post(wrap))
                await buffer.SendAsync(wrap);
            return await wrap.TaskSource.Task;
        }
        public int consuming = 0;
        public bool InConsuming => consuming != 0;
        public async void StartConsumer(object state)
        {
            if (Interlocked.CompareExchange(ref consuming, 1, 0) == 0)
            {
                try
                {
                    while (await buffer.OutputAvailableAsync())
                    {
                        await consumer(buffer);
                    }
                }
                catch
                {
                    //TODO 错误日志输出
                }
                finally
                {
                    Interlocked.Exchange(ref consuming, 0);
                }
            }
        }
        public bool IsComplete { get; private set; }
        public void Complete()
        {
            IsComplete = true;
            buffer.Complete();
        }
    }
    public class DataTaskWrapper<T, R>
    {
        public DataTaskWrapper(T data)
        {
            Value = data;
        }
        public TaskCompletionSource<R> TaskSource { get; set; } = new TaskCompletionSource<R>();
        public T Value { get; set; }
    }
}
