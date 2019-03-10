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
        readonly IOptions<ChannelOptions> options;
        /// <summary>
        /// 是否在自动消费中
        /// </summary>
        private int _autoConsuming = 0;
        public MpscChannel(ILogger<MpscChannel<T>> logger, IOptions<ChannelOptions> options)
        {
            this.logger = logger;
            this.options = options;
        }
        public bool IsComplete { get; private set; }
        public bool IsChildren { get; set; }

        public IMpscChannel<T> BindConsumer(Func<List<T>, Task> consumer)
        {
            if (this.consumer == default)
                this.consumer = consumer;
            else
                throw new RebindConsumerException(GetType().FullName);
            return this;
        }

        public async ValueTask<bool> WriteAsync(T data)
        {
            if (!IsChildren && !(_autoConsuming != 0))
                ActiveAutoConsumer();
            if (!buffer.Post(data))
                return await buffer.SendAsync(data);
            return true;
        }
        private void ActiveAutoConsumer()
        {
            if (!IsChildren && !(_autoConsuming != 0))
                ThreadPool.QueueUserWorkItem(ActiveConsumer);
            async void ActiveConsumer(object state)
            {
                if (Interlocked.CompareExchange(ref _autoConsuming, 1, 0) == 0)
                {
                    try
                    {
                        while (await WaitToReadAsync())
                        {
                            await ManualConsume();
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, ex.Message);
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
                while (buffer.TryReceive(out var value))
                {
                    dataList.Add(value);
                    if (dataList.Count > options.Value.MaxSizeOfBatch) break;
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
