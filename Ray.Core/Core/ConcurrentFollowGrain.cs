using System;
using System.Collections.Generic;
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

namespace Ray.Core
{
    public abstract class ConcurrentFollowGrain<Main, PrimaryKey> : FollowGrain<Main, PrimaryKey>, IConcurrentFollow
    {
        readonly List<IFullyEvent<PrimaryKey>> UnprocessedEventList = new List<IFullyEvent<PrimaryKey>>();
        public ConcurrentFollowGrain(ILogger logger) : base(logger)
        {
        }
        /// <summary>
        /// 多生产者单消费者消息信道
        /// </summary>
        protected IMpscChannel<DataAsyncWrapper<IFullyEvent<PrimaryKey>, bool>> ConcurrentChannel { get; private set; }
        protected override bool EventConcurrentProcessing => true;
        public override Task OnActivateAsync()
        {
            ConcurrentChannel = ServiceProvider.GetService<IMpscChannel<DataAsyncWrapper<IFullyEvent<PrimaryKey>, bool>>>().BindConsumer(BatchInputProcessing);
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
            var (success, transport) = EventBytesTransport.FromBytesWithNoId(bytes);
            if (success)
            {
                var data = Serializer.Deserialize(TypeContainer.GetType(transport.EventType), transport.EventBytes);
                if (data is IEvent @event)
                {
                    var eventBase = EventBase.FromBytes(transport.BaseBytes);
                    if (eventBase.Version > Snapshot.Version)
                    {
                        var writeTask = ConcurrentChannel.WriteAsync(new DataAsyncWrapper<IFullyEvent<PrimaryKey>, bool>(new FullyEvent<PrimaryKey>
                        {
                            StateId = GrainId,
                            Base = eventBase,
                            Event = @event
                        }));
                        if (!writeTask.IsCompletedSuccessfully)
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
                else
                {
                    if (Logger.IsEnabled(LogLevel.Information))
                        Logger.LogInformation(LogEventIds.FollowEventProcessing, "Receive non-event messages, grain Id = {0} ,message type = {1}", GrainId.ToString(), transport.EventType);
                }
            }
        }
        readonly TimeoutException timeoutException = new TimeoutException($"{nameof(OnEventDelivered)} with timeouts in {nameof(BatchInputProcessing)}");
        private async Task BatchInputProcessing(List<DataAsyncWrapper<IFullyEvent<PrimaryKey>, bool>> events)
        {
            var evtList = new List<IFullyEvent<PrimaryKey>>();
            var startVersion = Snapshot.Version;
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
                    if (startVersion + orderList.Count < inputLast.Base.Version)
                    {
                        var loadList = await EventStorage.GetList(GrainId, 0, startVersion + 1, inputLast.Base.Version - 1);
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
                        var allTask = Task.WhenAll(UnprocessedEventList.Select(@event =>
                        {
                            var task = OnEventDelivered(@event);
                            if (!task.IsCompletedSuccessfully)
                                return task.AsTask();
                            else
                                return Task.CompletedTask;
                        }));
                        using (var delayTask = Task.Delay(ConfigOptions.EventAsyncProcessTimeoutSeconds, tokenSource.Token))
                        {
                            await Task.WhenAny(allTask, delayTask);
                            if (allTask.Status == TaskStatus.RanToCompletion)
                            {
                                tokenSource.Cancel();
                                var lastEvt = UnprocessedEventList.Last();
                                var lastEvtBase = lastEvt.Base;
                                Snapshot.UnsafeUpdateVersion(lastEvtBase);
                                var saveTask = SaveSnapshotAsync();
                                if (!saveTask.IsCompletedSuccessfully)
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
