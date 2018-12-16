using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;

namespace Ray.Core.Messaging.Channels
{
    /// <summary>
    /// multi producter single consumer channel
    /// </summary>
    /// <typeparam name="T">data type produced by producer</typeparam>
    public class MpscChannel<T> : IMpscChannel<T>
    {
        readonly BufferBlock<T> buffer = new BufferBlock<T>();
        readonly Func<List<T>, Task> consumer;
        readonly List<IMpscChannelBase> consumerSequence = new List<IMpscChannelBase>();
        private Task<bool> waitToReadTask;
        readonly ILogger logger;
        readonly int maxDataCountPerBatch;
        public MpscChannel(ILogger logger, Func<List<T>, Task> consumer, int maxDataCountPerBatch = 5000)
        {
            this.logger = logger;
            this.consumer = consumer;
            this.maxDataCountPerBatch = maxDataCountPerBatch;
        }

        public async ValueTask<bool> WriteAsync(T data)
        {
            if (!buffer.Post(data))
                return await buffer.SendAsync(data);
            return true;
        }
        public void JoinConsumerSequence(IMpscChannelBase channel)
        {
            if (consumerSequence.IndexOf(channel) == -1)
                consumerSequence.Add(channel);
        }
        public async Task Consume()
        {
            if (waitToReadTask.IsCompletedSuccessfully && waitToReadTask.Result)
            {
                var dataList = new List<T>();
                while (buffer.TryReceive(out var value))
                {
                    dataList.Add(value);
                    if (dataList.Count > maxDataCountPerBatch) break;
                }
                await consumer(dataList);
            }
            foreach (var joinConsumer in consumerSequence)
            {
                await joinConsumer.Consume();
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
        private int consuming = 0;
        public bool InConsuming => consuming != 0;
        private async void ActiveConsumer(object state)
        {
            if (Interlocked.CompareExchange(ref consuming, 1, 0) == 0)
            {
                try
                {
                    while (await WaitToReadAsync())
                    {
                        await Consume();
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.Message);
                }
                finally
                {
                    Interlocked.Exchange(ref consuming, 0);
                }
            }
        }
        public bool ActiveConsumer()
        {
            return ThreadPool.QueueUserWorkItem(ActiveConsumer);
        }
        public bool IsComplete { get; private set; }
        public void Complete()
        {
            IsComplete = true;
            buffer.Complete();
        }
    }
}
