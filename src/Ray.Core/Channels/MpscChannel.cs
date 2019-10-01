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
        /// <summary>
        /// 是否在自动消费中
        /// </summary>
        private int _autoConsuming = 0;
        public MpscChannel(ILogger<MpscChannel<T>> logger, IOptions<ChannelOptions> options)
        {
            this.logger = logger;
            MaxBatchSize = options.Value.MaxBatchSize;
            MaxMillisecondsDelay = options.Value.MaxMillisecondsDelay;
        }
        /// <summary>
        /// 批量数据处理每次处理的最大数据量
        /// </summary>
        public int MaxBatchSize { get; set; }
        /// <summary>
        /// 批量数据接收的最大延时
        /// </summary>
        public int MaxMillisecondsDelay { get; set; }
        public bool IsComplete { get; private set; }
        public bool IsChildren { get; set; }

        public void BindConsumer(Func<List<T>, Task> consumer)
        {
            if (this.consumer is null)
                this.consumer = consumer;
            else
                throw new RebindConsumerException(GetType().Name);
        }
        public void BindConsumer(Func<List<T>, Task> consumer, int maxBatchSize, int maxMillisecondsDelay)
        {
            if (this.consumer is null)
            {
                this.consumer = consumer;
                MaxBatchSize = maxBatchSize;
                MaxMillisecondsDelay = maxMillisecondsDelay;
            }
            else
                throw new RebindConsumerException(GetType().Name);
        }
        public void Config(int maxBatchSize, int maxMillisecondsDelay)
        {
            MaxBatchSize = maxBatchSize;
            MaxMillisecondsDelay = maxMillisecondsDelay;
        }
        public async ValueTask<bool> WriteAsync(T data)
        {
            if (consumer is null)
                throw new NoBindConsumerException(GetType().Name);
            if (!IsChildren && _autoConsuming == 0)
                ActiveAutoConsumer();
            if (!buffer.Post(data))
                return await buffer.SendAsync(data);
            return true;
        }
        private void ActiveAutoConsumer()
        {
            if (!IsChildren && _autoConsuming == 0)
                ThreadPool.QueueUserWorkItem(ActiveConsumer);
            async void ActiveConsumer(object state)
            {
                if (Interlocked.CompareExchange(ref _autoConsuming, 1, 0) == 0)
                {
                    try
                    {
                        while (await WaitToReadAsync())
                        {
                            try
                            {
                                await ManualConsume();
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, ex.Message);
                            }
                        }
                    }
                    finally
                    {
                        Interlocked.Exchange(ref _autoConsuming, 0);
                    }
                }
            }
        }
        public void JoinConsumerSequence(IBaseMpscChannel channel)
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
                    if (dataList.Count > MaxBatchSize)
                    {
                        break;
                    }
                    else if ((DateTimeOffset.UtcNow - startTime).TotalMilliseconds > MaxMillisecondsDelay)
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
        public void Complete()
        {
            IsComplete = true;
            foreach (var joinConsumer in consumerSequence)
            {
                joinConsumer.Complete();
            }
            buffer.Complete();
        }
    }
}
