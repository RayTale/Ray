using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Exceptions;
using Ray.Core.Messaging;
using Ray.Core.Messaging.Channels;

namespace Ray.Core.Internal
{
    public abstract class ConcurrentFollowGrain<K, S, W> : FollowGrain<K, S, W>
          where S : class, IState<K>, new()
          where W : IBytesMessage
    {
        readonly List<IEventBase<K>> UnprocessedEventList = new List<IEventBase<K>>();
        public ConcurrentFollowGrain(ILogger logger) : base(logger)
        {
        }

        /// <summary>
        /// 多生产者单消费者消息信道
        /// </summary>
        protected IMpscChannel<MessageTaskSource<IEventBase<K>, bool>> MpscChannel { get; private set; }
        protected override bool EventConcurrentProcessing => true;
        public override Task OnActivateAsync()
        {
            MpscChannel = ServiceProvider.GetService<IMpscChannelFactory<K, MessageTaskSource<IEventBase<K>, bool>>>().Create(Logger, GrainId, BatchInputProcessing, ConfigOptions.MaxSizeOfPerBatch);
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            MpscChannel.Complete();
            return base.OnDeactivateAsync();
        }
        public async Task ConcurrentTell(byte[] bytes)
        {
            using (var wms = new MemoryStream(bytes))
            {
                var message = Serializer.Deserialize<W>(wms);
                using (var ems = new MemoryStream(message.Bytes))
                {
                    if (Serializer.Deserialize(TypeContainer.GetType(message.TypeName), ems) is IEventBase<K> @event)
                    {
                        if (@event.Version > State.Version)
                        {
                            var writeTask = MpscChannel.WriteAsync(new MessageTaskSource<IEventBase<K>, bool>(@event));
                            if (!writeTask.IsCompleted)
                                await writeTask;
                            if (!writeTask.Result)
                            {
                                var ex = new ChannelUnavailabilityException(GrainId.ToString(), GrainType);
                                if (Logger.IsEnabled(LogLevel.Error))
                                    Logger.LogError(LogEventIds.TransactionGrainCurrentInput, ex, ex.Message);
                                throw ex;
                            }
                        }
                    }
                }
            }
        }
        readonly TimeoutException timeoutException = new TimeoutException($"{nameof(OnEventDelivered)} with timeouts in {nameof(BatchInputProcessing)}");
        private async Task BatchInputProcessing(List<MessageTaskSource<IEventBase<K>, bool>> events)
        {
            var start = DateTime.UtcNow;
            var evtList = new List<IEventBase<K>>();
            var startVersion = State.Version;
            var startTime = State.VersionTime;
            if (UnprocessedEventList.Count > 0)
            {
                var startEvt = UnprocessedEventList.Last();
                startVersion = startEvt.Version;
                startTime = startEvt.Timestamp;
            }
            var maxVersion = startVersion;
            TaskCompletionSource<bool> maxRequest = default;
            try
            {
                foreach (var wrap in events)
                {
                    if (wrap.Value.Version <= startVersion)
                    {
                        wrap.TaskSource.TrySetResult(true);
                    }
                    else
                    {
                        evtList.Add(wrap.Value);
                        if (wrap.Value.Version > maxVersion)
                        {
                            maxRequest?.TrySetResult(true);
                            maxVersion = wrap.Value.Version;
                            maxRequest = wrap.TaskSource;
                        }
                        else
                        {
                            wrap.TaskSource.TrySetResult(true);
                        }
                    }
                }
                var orderList = evtList.OrderBy(w => w.Version).ToList();
                if (orderList.Count > 0)
                {
                    var inputLast = orderList.Last();
                    if (startVersion + orderList.Count != inputLast.Version)
                    {
                        var eventStorageTask = GetEventStorage();
                        if (!eventStorageTask.IsCompleted)
                            await eventStorageTask;
                        var loadList = await eventStorageTask.Result.GetListAsync(GrainId, startVersion, inputLast.Version, startTime);
                        UnprocessedEventList.AddRange(loadList);
                    }
                    else
                    {
                        UnprocessedEventList.AddRange(orderList.Select(w => w));
                    }
                }
                if (UnprocessedEventList.Count > 0)
                {
                    using (var tokenSource = new CancellationTokenSource())
                    {
                        var tasks = UnprocessedEventList.Select(@event =>
                        {
                            var task = OnEventDelivered(@event);
                            if (!task.IsCompleted)
                                return task.AsTask();
                            else
                                return Task.CompletedTask;
                        });
                        var taskOne = Task.WhenAll(tasks);
                        using (var taskTwo = Task.Delay(EventAsyncProcessTimeoutSeconds, tokenSource.Token))
                        {
                            await Task.WhenAny(taskOne, taskTwo);
                            if (taskOne.Status == TaskStatus.RanToCompletion)
                            {
                                tokenSource.Cancel();
                                var lastEvt = UnprocessedEventList.Last();
                                State.UnsafeUpdateVersion(lastEvt.Version, lastEvt.Timestamp);
                                var saveTask = SaveSnapshotAsync();
                                if (!saveTask.IsCompleted)
                                    await saveTask;
                                UnprocessedEventList.Clear();
                                maxRequest?.TrySetResult(true);
                            }
                            else
                            {
                                maxRequest?.TrySetException(timeoutException);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsEnabled(LogLevel.Error))
                    Logger.LogError(LogEventIds.FollowGrainEventHandling, ex, "FollowGrain event handling failed, type {0} with Id {1}", GrainType.FullName, GrainId.ToString());
                maxRequest?.TrySetException(ex);
            }
        }

    }
}
