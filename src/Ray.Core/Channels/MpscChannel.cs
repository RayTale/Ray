using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ray.Core.Channels
{
    /// <summary>
    /// multi producter single consumer channel
    /// </summary>
    /// <typeparam name="T">data type produced by producer</typeparam>
    public class MpscChannel<T> : IMpscChannel<T>
    {
        readonly BufferBlock<T> buffer = new BufferBlock<T>();
        private Func<List<T>, Task> consumer;
        readonly List<IBaseMpscChannel> consumerSequence = new List<IBaseMpscChannel>();
        private Task<bool> waitToReadTask;
        readonly ILogger logger;
        private Task consumerTask;
        /// <summary>
        /// 批量数据处理每次处理的最大数据量
        /// </summary>
        int _MaxBatchSize;
        /// <summary>
        /// 批量数据接收的最大延时
        /// </summary>
        int _MaxMillisecondsDelay;
        public MpscChannel(ILogger<MpscChannel<T>> logger, IOptions<ChannelOptions> options)
        {
            this.logger = logger;
            _MaxBatchSize = options.Value.MaxBatchSize;
            _MaxMillisecondsDelay = options.Value.MaxMillisecondsDelay;
        }
        public bool IsDisposed { get; private set; }
        public bool IsChildren { get; set; }

        public void BindConsumer(Func<List<T>, Task> consumer)
        {
            if (Interlocked.CompareExchange(ref this.consumer, consumer, null) is null)
            {
                this.consumer = consumer;
                consumerTask = ActiveAutoConsumer();
            }
            else
                throw new RebindConsumerException(GetType().Name);
        }
        public void Config(int maxBatchSize, int maxMillisecondsDelay)
        {
            _MaxBatchSize = maxBatchSize;
            _MaxMillisecondsDelay = maxMillisecondsDelay;
        }
        public async ValueTask<bool> WriteAsync(T data)
        {
            if (consumer is null)
                throw new NoBindConsumerException(GetType().Name);
            if (!buffer.Post(data))
                return await buffer.SendAsync(data);
            return true;
        }
        private async Task ActiveAutoConsumer()
        {
            while (!IsDisposed)
            {
                try
                {
                    await WaitToReadAsync();
                    await ManualConsume();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.Message);
                }
            }
        }
        public void Join(IBaseMpscChannel channel)
        {
            if (consumerSequence.IndexOf(channel) == -1)
            {
                channel.IsChildren = true;
                consumerSequence.Add(channel);
            }
        }
        public async Task ManualConsume()
        {
            if (waitToReadTask.IsCompletedSuccessfully && waitToReadTask.Result)
            {
                var dataList = new List<T>();
                var startTime = DateTimeOffset.UtcNow;
                while (buffer.TryReceive(out var value))
                {
                    dataList.Add(value);
                    if (dataList.Count > _MaxBatchSize)
                    {
                        break;
                    }
                    else if ((DateTimeOffset.UtcNow - startTime).TotalMilliseconds > _MaxMillisecondsDelay)
                    {
                        break;
                    }
                }
                if (dataList.Count > 0)
                    await consumer(dataList);
            }
            foreach (var joinConsumer in consumerSequence)
            {
                await joinConsumer.ManualConsume();
            }
        }
        public async Task<bool> WaitToReadAsync()
        {
            waitToReadTask = buffer.OutputAvailableAsync();
            if (consumerSequence.Count == 0)
            {
                return await waitToReadTask;
            }
            else
            {
                var taskList = consumerSequence.Select(c => c.WaitToReadAsync()).ToList();
                taskList.Add(waitToReadTask);
                return await await Task.WhenAny(taskList);
            }
        }
        public void Dispose()
        {
            IsDisposed = true;
            foreach (var joinConsumer in consumerSequence)
            {
                joinConsumer.Dispose();
            }
            buffer.Complete();
        }
    }
}
