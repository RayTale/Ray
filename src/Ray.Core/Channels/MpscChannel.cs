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
    public class MpscChannel<T> : IMpscChannel<T>, ISequenceMpscChannel
    {
        private readonly BufferBlock<T> buffer = new BufferBlock<T>();
        private Func<List<T>, Task> consumer;
        private readonly List<ISequenceMpscChannel> consumerSequence = new List<ISequenceMpscChannel>();
        private Task<bool> waitToReadTask;
        private readonly ILogger logger;
        private Task consumerTask;

        /// <summary>
        /// 批量数据处理每次处理的最大数据量
        /// </summary>
        private int MaxBatchSize;

        /// <summary>
        /// 批量数据接收的最大延时
        /// </summary>
        private int MaxMillisecondsDelay;

        public MpscChannel(ILogger<MpscChannel<T>> logger, IOptions<ChannelOptions> options)
        {
            this.logger = logger;
            this.MaxBatchSize = options.Value.MaxBatchSize;
            this.MaxMillisecondsDelay = options.Value.MaxMillisecondsDelay;
        }

        public bool IsDisposed { get; private set; }

        public bool IsChildren { get; set; }

        public void BindConsumer(Func<List<T>, Task> consumer, bool active = true)
        {
            if (active)
            {
                if (Interlocked.CompareExchange(ref this.consumer, consumer, null) is null)
                {
                    this.consumer = consumer;
                    this.consumerTask = this.ActiveAutoConsumer();
                }
                else
                {
                    throw new RebindConsumerException(this.GetType().Name);
                }
            }
        }

        public void Config(int maxBatchSize, int maxMillisecondsDelay)
        {
            this.MaxBatchSize = maxBatchSize;
            this.MaxMillisecondsDelay = maxMillisecondsDelay;
        }

        public async ValueTask<bool> WriteAsync(T data)
        {
            if (this.consumer is null)
            {
                throw new NoBindConsumerException(this.GetType().Name);
            }

            if (!this.buffer.Post(data))
            {
                return await this.buffer.SendAsync(data);
            }

            return true;
        }

        private async Task ActiveAutoConsumer()
        {
            while (!this.IsDisposed)
            {
                try
                {
                    await this.WaitToReadAsync();
                    await this.ManualConsume();
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, ex.Message);
                }
            }
        }

        public void Join(ISequenceMpscChannel channel)
        {
            if (this.consumerSequence.IndexOf(channel) == -1)
            {
                channel.IsChildren = true;
                this.consumerSequence.Add(channel);
            }
        }

        public async Task ManualConsume()
        {
            if (this.waitToReadTask.IsCompletedSuccessfully && this.waitToReadTask.Result)
            {
                var dataList = new List<T>();
                var startTime = DateTimeOffset.UtcNow;
                while (this.buffer.TryReceive(out var value))
                {
                    dataList.Add(value);
                    if (dataList.Count > this.MaxBatchSize)
                    {
                        break;
                    }
                    else if ((DateTimeOffset.UtcNow - startTime).TotalMilliseconds > this.MaxMillisecondsDelay)
                    {
                        break;
                    }
                }

                if (dataList.Count > 0)
                {
                    await this.consumer(dataList);
                }
            }

            foreach (var joinConsumer in this.consumerSequence)
            {
                await joinConsumer.ManualConsume();
            }
        }

        public async Task<bool> WaitToReadAsync()
        {
            this.waitToReadTask = this.buffer.OutputAvailableAsync();
            if (this.consumerSequence.Count == 0)
            {
                return await this.waitToReadTask;
            }
            else
            {
                var taskList = this.consumerSequence.Select(c => c.WaitToReadAsync()).ToList();
                taskList.Add(this.waitToReadTask);
                return await await Task.WhenAny(taskList);
            }
        }

        public void Dispose()
        {
            this.IsDisposed = true;
            foreach (var joinConsumer in this.consumerSequence)
            {
                joinConsumer.Dispose();
            }

            this.buffer.Complete();
        }
    }
}
