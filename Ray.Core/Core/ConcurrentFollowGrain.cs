using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Logging;
using Ray.Core.Serialization;
using Ray.Core.State;

namespace Ray.Core
{
    public abstract class ConcurrentFollowGrain<K, E, S, W> : FollowGrain<K, E, S, W>
          where E : IEventBase<K>
          where S : class, IActorState<K>, new()
          where W : IBytesWrapper
    {
        readonly List<IEvent<K, E>> UnprocessedEventList = new List<IEvent<K, E>>();
        public ConcurrentFollowGrain(ILogger logger) : base(logger)
        {
        }

        /// <summary>
        /// 多生产者单消费者消息信道
        /// </summary>
        protected IMpscChannel<DataAsyncWrapper<IEvent<K, E>, bool>> ConcurrentChannel { get; private set; }
        protected override bool EventConcurrentProcessing => true;
        public override Task OnActivateAsync()
        {
            ConcurrentChannel = ServiceProvider.GetService<IMpscChannel<DataAsyncWrapper<IEvent<K, E>, bool>>>().BindConsumer(BatchInputProcessing);
            ConcurrentChannel.ActiveConsumer();
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            ConcurrentChannel.Complete();
            return base.OnDeactivateAsync();
        }
        public async Task ConcurrentTell(byte[] bytes)
        {
            using (var wms = new MemoryStream(bytes))
            {
                var message = Serializer.Deserialize<W>(wms);
                using (var ems = new MemoryStream(message.Bytes))
                {
                    if (Serializer.Deserialize(TypeContainer.GetType(message.TypeName), ems) is IEvent<K, E> @event)
                    {
                        if (@event.Base.Version > State.Version)
                        {
                            var writeTask = ConcurrentChannel.WriteAsync(new DataAsyncWrapper<IEvent<K, E>, bool>(@event));
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
        private async Task BatchInputProcessing(List<DataAsyncWrapper<IEvent<K, E>, bool>> events)
        {
            var evtList = new List<IEvent<K, E>>();
            var startVersion = State.Version;
            if (UnprocessedEventList.Count > 0)
            {
                startVersion = UnprocessedEventList.Last().Base.Version;
            }
            var maxVersion = startVersion;
            TaskCompletionSource<bool> maxRequest = default;
            try
            {
                foreach (var wrap in events)
                {
                    if (wrap.Value.Base.Version <= startVersion)
                    {
                        wrap.TaskSource.TrySetResult(true);
                    }
                    else
                    {
                        evtList.Add(wrap.Value);
                        if (wrap.Value.Base.Version > maxVersion)
                        {
                            maxRequest?.TrySetResult(true);
                            maxVersion = wrap.Value.Base.Version;
                            maxRequest = wrap.TaskSource;
                        }
                        else
                        {
                            wrap.TaskSource.TrySetResult(true);
                        }
                    }
                }
                var orderList = evtList.OrderBy(e => e.Base.Version).ToList();
                if (orderList.Count > 0)
                {
                    var inputLast = orderList.Last();
                    if (startVersion + orderList.Count != inputLast.Base.Version)
                    {
                        var loadList = await EventStorage.GetListAsync(GrainId, startVersion, inputLast.Base.Version);
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
                                State.UnsafeUpdateVersion(lastEvt.Base.Version, lastEvt.Base.Timestamp);
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
                    Logger.LogError(LogEventIds.FollowGrainEventHandling, ex, "FollowGrain event handling failed with Id {1}", GrainId.ToString());
                maxRequest?.TrySetException(ex);
            }
        }
    }
}
