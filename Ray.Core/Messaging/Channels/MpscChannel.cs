using System;
using System.Linq;
using System.Collections.Concurrent;
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
    /// <typeparam name="R">data type returned after processing</typeparam>
    public class MpscChannel<T, R> : IMpscChannel<T, R>
    {
        readonly BufferBlock<MessageTaskWrapper<T, R>> buffer = new BufferBlock<MessageTaskWrapper<T, R>>();
        readonly Func<List<MessageTaskWrapper<T, R>>, Task> consumer;
        readonly List<IMpscChannelBase> consumerSequence = new List<IMpscChannelBase>();
        private Task<bool> waitToReadTask;
        readonly ILogger logger;
        readonly int maxPerBatch;
        public MpscChannel(ILogger logger, Func<List<MessageTaskWrapper<T, R>>, Task> consumer, int maxPerBatch = 5000)
        {
            this.logger = logger;
            this.consumer = consumer;
            this.maxPerBatch = maxPerBatch;
        }

        public async Task<R> WriteAsync(T data)
        {
            var wrap = new MessageTaskWrapper<T, R>(data);
            if (!buffer.Post(wrap))
                await buffer.SendAsync(wrap);
            return await wrap.TaskSource.Task;
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
                var dataList = new List<MessageTaskWrapper<T, R>>();
                while (buffer.TryReceive(out var value))
                {
                    dataList.Add(value);
                    if (dataList.Count > maxPerBatch) break;
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
