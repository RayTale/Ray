using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Ray.Core.Utils
{
    public class ChannelContainer<K, T, R>
    {
        readonly ConcurrentDictionary<K, DataBatchChannel<T, R>> insertChannelDict = new ConcurrentDictionary<K, DataBatchChannel<T, R>>();
        readonly Func<BufferBlock<DataTaskWrap<T, R>>, Task> Process;
        public ChannelContainer(Func<BufferBlock<DataTaskWrap<T, R>>, Task> process)
        {
            Process = process;
        }
        public DataBatchChannel<T, R> GetChannel(K key)
        {
            if (!insertChannelDict.TryGetValue(key, out var channel))
            {
                channel = new DataBatchChannel<T, R>(Process);
                if (!insertChannelDict.TryAdd(key, channel))
                {
                    channel = insertChannelDict[key];
                }
            }
            return channel;
        }
    }
    public class DataBatchChannel<T, R>
    {
        readonly BufferBlock<DataTaskWrap<T, R>> flowChannel = new BufferBlock<DataTaskWrap<T, R>>();
        readonly Func<BufferBlock<DataTaskWrap<T, R>>, Task> Process;
        public DataBatchChannel(Func<BufferBlock<DataTaskWrap<T, R>>, Task> process)
        {
            Process = process;
        }
        public async Task<R> WriteAsync(T data)
        {
            var wrap = new DataTaskWrap<T, R>(data);
            if (!flowChannel.Post(wrap))
                await flowChannel.SendAsync(wrap);
            if (isProcessing == 0)
                TriggerFlowProcess();
            return await wrap.TaskSource.Task;
        }
        int isProcessing = 0;
        private async void TriggerFlowProcess()
        {
            if (Interlocked.CompareExchange(ref isProcessing, 1, 0) == 0)
            {
                try
                {
                    while (await flowChannel.OutputAvailableAsync())
                    {
                        await Process(flowChannel);
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref isProcessing, 0);
                }
            }
        }
        public void Complete()
        {
            flowChannel.Complete();
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
