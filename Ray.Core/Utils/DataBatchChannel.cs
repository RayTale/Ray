using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Ray.Core.Utils
{
    public class ChannelContainer<K, T, R>
    {
        readonly ConcurrentDictionary<K, DataBatchChannel<T, R>> insertChannelDict = new ConcurrentDictionary<K, DataBatchChannel<T, R>>();
        readonly Func<DataTaskWrap<T, R>, ChannelReader<DataTaskWrap<T, R>>, ValueTask> Process;
        public ChannelContainer(Func<DataTaskWrap<T, R>, ChannelReader<DataTaskWrap<T, R>>, ValueTask> process)
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
        Channel<DataTaskWrap<T, R>> flowChannel = Channel.CreateUnbounded<DataTaskWrap<T, R>>();
        readonly Func<DataTaskWrap<T, R>, ChannelReader<DataTaskWrap<T, R>>, ValueTask> Process;
        public DataBatchChannel(Func<DataTaskWrap<T, R>, ChannelReader<DataTaskWrap<T, R>>, ValueTask> process)
        {
            Process = process;
        }
        public async ValueTask<R> WriteAsync(T data)
        {
            var wrap = new DataTaskWrap<T, R>(data);
            await flowChannel.Writer.WriteAsync(wrap);
            if (isProcessing == 0)
                TriggerFlowProcess().GetAwaiter();
            return await wrap.TaskSource.Task;
        }
        int isProcessing = 0;
        private async ValueTask TriggerFlowProcess()
        {
            if (Interlocked.CompareExchange(ref isProcessing, 1, 0) == 0)
            {
                while (await flowChannel.Reader.WaitToReadAsync())
                {
                    while (await FlowProcess()) { }
                }
                Interlocked.Exchange(ref isProcessing, 0);
            }
        }
        private async ValueTask<bool> FlowProcess()
        {
            if (flowChannel.Reader.TryRead(out var first))
            {
                await Process(first, flowChannel.Reader);
                //返回true代表有接收到数据
                return true;
            }
            //没有接收到数据
            return false;
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
    public class DataBatchChannel<T>
    {
        Channel<T> flowChannel = Channel.CreateUnbounded<T>();
        readonly Func<T, ChannelReader<T>, ValueTask> Process;
        public DataBatchChannel(Func<T, ChannelReader<T>, ValueTask> process)
        {
            Process = process;
        }
        public async ValueTask WriteAsync(T data)
        {
            await flowChannel.Writer.WriteAsync(data);
            if (isProcessing == 0)
                TriggerFlowProcess().GetAwaiter();
        }
        int isProcessing = 0;
        private async ValueTask TriggerFlowProcess()
        {
            if (Interlocked.CompareExchange(ref isProcessing, 1, 0) == 0)
            {
                while (await flowChannel.Reader.WaitToReadAsync())
                {
                    while (await FlowProcess()) { }
                }
                Interlocked.Exchange(ref isProcessing, 0);
            }
        }
        private async ValueTask<bool> FlowProcess()
        {
            if (flowChannel.Reader.TryRead(out var first))
            {
                await Process(first, flowChannel.Reader);
                //返回true代表有接收到数据
                return true;
            }
            //没有接收到数据
            return false;
        }
    }
}
