﻿using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Exceptions;
using Ray.Core.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Ray.Core
{
    public abstract class ConcurrentObserverGrain<Main, PrimaryKey> : ObserverGrain<Main, PrimaryKey>, IConcurrentObserver
    {
        readonly List<IFullyEvent<PrimaryKey>> UnprocessedEventList = new List<IFullyEvent<PrimaryKey>>();
        /// <summary>
        /// 多生产者单消费者消息信道
        /// </summary>
        protected IMpscChannel<AsyncInputEvent<IFullyEvent<PrimaryKey>, bool>> ConcurrentChannel { get; private set; }
        protected override bool ConcurrentHandle => true;
        public override Task OnActivateAsync()
        {
            ConcurrentChannel = ServiceProvider.GetService<IMpscChannel<AsyncInputEvent<IFullyEvent<PrimaryKey>, bool>>>().BindConsumer(BatchInputProcessing);
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            ConcurrentChannel.Complete();
            return base.OnDeactivateAsync();
        }
        public async Task ConcurrentOnNext(Immutable<byte[]> bytes)
        {
            var (success, transport) = EventBytesTransport.FromBytesWithNoId(bytes.Value);
            if (success)
            {
                var data = Serializer.Deserialize(TypeContainer.GetType(transport.EventTypeCode), transport.EventBytes);
                if (data is IEvent @event)
                {
                    var eventBase = EventBase.FromBytes(transport.BaseBytes);
                    if (eventBase.Version > Snapshot.Version)
                    {
                        var input = new AsyncInputEvent<IFullyEvent<PrimaryKey>, bool>(new FullyEvent<PrimaryKey>
                        {
                            StateId = GrainId,
                            Base = eventBase,
                            Event = @event
                        });
                        var writeTask = ConcurrentChannel.WriteAsync(input);
                        if (!writeTask.IsCompletedSuccessfully)
                            await writeTask;
                        if (!writeTask.Result)
                        {
                            var ex = new ChannelUnavailabilityException(GrainId.ToString(), GrainType);
                            Logger.LogError(ex, ex.Message);
                            throw ex;
                        }
                        await input.TaskSource.Task;
                    }
                }
                else
                {
                    if (Logger.IsEnabled(LogLevel.Information))
                        Logger.LogInformation("Receive non-event messages, grain Id = {0} ,message type = {1}", GrainId.ToString(), transport.EventTypeCode);
                }
            }
        }
        private async Task BatchInputProcessing(List<AsyncInputEvent<IFullyEvent<PrimaryKey>, bool>> eventInputs)
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
                foreach (var input in eventInputs)
                {
                    if (input.Value.Base.Version == startVersion)
                    {
                        maxRequest = input.TaskSource;
                    }
                    else if (input.Value.Base.Version < startVersion)
                    {
                        input.TaskSource.TrySetResult(true);
                    }
                    else
                    {
                        evtList.Add(input.Value);
                        if (input.Value.Base.Version > maxVersion)
                        {
                            maxRequest?.TrySetResult(true);
                            maxVersion = input.Value.Base.Version;
                            maxRequest = input.TaskSource;
                        }
                        else
                        {
                            input.TaskSource.TrySetResult(true);
                        }
                    }
                }

                if (evtList.Count > 0)
                {
                    var orderList = evtList.OrderBy(e => e.Base.Version).ToList();
                    var inputLast = orderList.Last();
                    if (startVersion + orderList.Count < inputLast.Base.Version)
                    {
                        var loadList = await EventStorage.GetList(GrainId, 0, startVersion + 1, inputLast.Base.Version - 1);
                        UnprocessedEventList.AddRange(loadList);
                        UnprocessedEventList.Add(inputLast);
                    }
                    else
                    {
                        UnprocessedEventList.AddRange(orderList.Select(w => w));
                    }
                }
                if (UnprocessedEventList.Count > 0)
                {
                    await Task.WhenAll(UnprocessedEventList.Select(async @event =>
                    {
                        var task = EventDelivered(@event);
                        if (!task.IsCompletedSuccessfully)
                            await task;
                    }));
                    Snapshot.UnsafeUpdateVersion(UnprocessedEventList.Last().Base);
                    var saveTask = SaveSnapshotAsync();
                    if (!saveTask.IsCompletedSuccessfully)
                        await saveTask;
                    UnprocessedEventList.Clear();
                    maxRequest?.TrySetResult(true);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "FollowGrain event handling failed with Id {1}", GrainId.ToString());
                maxRequest?.TrySetException(ex);
            }
        }
    }
}
