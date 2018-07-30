using Ray.Core.Utils;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using Ray.Core.Message;
using System.Threading.Tasks.Dataflow;
using System.Runtime.ExceptionServices;

namespace Ray.Core.EventSourcing
{
    public abstract class TransactionGrain<K, S, W> : ESGrain<K, S, W>
        where S : class, IState<K>, ITransactionable<S>, new()
        where W : IMessageWrapper, new()
    {
        protected S BackupState { get; set; }
        protected BufferBlock<EventTransactionWrap<K>> ConcurrentInputChannel { get; } = new BufferBlock<EventTransactionWrap<K>>();
        public override Task OnActivateAsync()
        {
            TriggerChannel();
            return base.OnActivateAsync();
        }
        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            ConcurrentInputChannel.Complete();
        }
        protected bool transactionPending = false;
        private long transactionStartVersion;
        private DateTime beginTransactionTime;
        private List<EventSaveWrap<K>> transactionEventList = new List<EventSaveWrap<K>>();
        protected override async ValueTask RecoveryState()
        {
            await base.RecoveryState();
            BackupState = State.DeepCopy();
        }
        protected async ValueTask BeginTransaction()
        {
            if (transactionPending)
            {
                if ((DateTime.UtcNow - beginTransactionTime).TotalMinutes > 1)
                {
                    await RollbackTransaction();//事务阻赛超过一分钟自动回滚
                }
                else
                    throw new Exception("The transaction has been opened");
            }
            await StateCheck();
            transactionPending = true;
            transactionStartVersion = State.Version;
            beginTransactionTime = DateTime.UtcNow;
        }
        protected async ValueTask CommitTransaction()
        {
            if (transactionEventList.Count > 0)
            {
                var serializer = GetSerializer();
                using (var ms = new PooledMemoryStream())
                {
                    foreach (var @event in transactionEventList)
                    {
                        serializer.Serialize(ms, @event.Evt);
                        @event.Bytes = ms.ToArray();
                        ms.Position = 0;
                        ms.SetLength(0);
                    }
                }
                await (await GetEventStorage()).TransactionSaveAsync(transactionEventList);
                if (SupportAsync)
                {
                    var mqService = GetMQService();
                    using (var ms = new PooledMemoryStream())
                    {
                        foreach (var @event in transactionEventList)
                        {
                            var data = new W
                            {
                                TypeCode = @event.Evt.TypeCode,
                                BinaryBytes = @event.Bytes
                            };
                            serializer.Serialize(ms, data);
                            mqService.Publish(ms.ToArray(), @event.HashKey);
                            OnRaiseSuccess(@event.Evt, @event.Bytes);
                            ms.Position = 0;
                            ms.SetLength(0);
                        }
                    }
                }
                else
                {
                    foreach (var evt in transactionEventList)
                    {
                        OnRaiseSuccess(evt.Evt, evt.Bytes);
                    }
                }
                transactionEventList.Clear();
                await SaveSnapshotAsync();
            }
            transactionPending = false;
        }
        protected async ValueTask RollbackTransaction()
        {
            if (transactionPending)
            {
                if (BackupState.Version == transactionStartVersion)
                {
                    State = BackupState.DeepCopy();
                }
                else
                {
                    await RecoveryState();
                }
                transactionEventList.Clear();
                transactionPending = false;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask StateCheck()
        {
            if (BackupState.Version != State.Version)
            {
                await RecoveryState();
                transactionEventList.Clear();
            }
        }
        protected override async ValueTask<bool> RaiseEvent(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            if (transactionPending)
                throw new Exception("The transaction has been opened,Please use RaiseEventByTransaction().");
            await StateCheck();
            return await base.RaiseEvent(@event, uniqueId, hashKey);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void OnRaiseSuccess(IEventBase<K> @event, byte[] bytes)
        {
            if (MessageTypeMapper.EventTypeDict.TryGetValue(@event.TypeCode, out var type))
            {
                using (var dms = new MemoryStream(bytes))
                {
                    Apply(BackupState, (IEventBase<K>)GetSerializer().Deserialize(type, dms));
                }
                BackupState.FullUpdateVersion(@event);//更新处理完成的Version
            }
        }
        protected void Transaction(IEventBase<K> @event, string uniqueId = null, string hashKey = null)
        {
            if (!transactionPending)
                throw new Exception("Unopened transaction,Please open the transaction first.");
            try
            {
                State.IncrementDoingVersion();//标记将要处理的Version
                @event.StateId = GrainId;
                @event.Version = State.Version + 1;
                @event.Timestamp = DateTime.UtcNow;
                transactionEventList.Add(new EventSaveWrap<K>(@event, uniqueId, string.IsNullOrEmpty(hashKey) ? GrainId.ToString() : hashKey));
                Apply(State, @event);
                State.UpdateVersion(@event);//更新处理完成的Version
            }
            catch (Exception ex)
            {
                State.DecrementDoingVersion();//还原doing Version
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
        protected async ValueTask<bool> ConcurrentInput(IEventBase<K> evt, string uniqueId = null)
        {
            var task = EventTransactionWrap<K>.Create(evt, uniqueId);
            if (flowProcess == 0)
            {
                TriggerChannel();
            }
            await ConcurrentInputChannel.SendAsync(task);

            return await task.TaskSource.Task;
        }
        int flowProcess = 0;
        protected async void TriggerChannel()
        {
            if (Interlocked.CompareExchange(ref flowProcess, 1, 0) == 0)
            {
                try
                {
                    while (true)
                    {
                        var (needTrigger, hasInput) = await WaitToReadAsync();
                        if (needTrigger)
                        {
                            if (hasInput)
                            {
                                await InputFlowBatchRaise();
                                await OnChannelNext(hasInput);
                            }
                            else
                            {
                                await OnChannelNext(hasInput);
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref flowProcess, 0);
                }
            }
        }
        protected virtual async ValueTask<(bool needTrigger, bool hasInput)> WaitToReadAsync()
        {
            return (await ConcurrentInputChannel.OutputAvailableAsync(), true);
        }
        protected virtual ValueTask OnChannelNext(bool hasInput)
        {
            return new ValueTask(Task.CompletedTask);
        }
        public async ValueTask InputFlowBatchRaise()
        {
            var start = DateTime.UtcNow;
            var events = new List<EventTransactionWrap<K>>();
            await BeginTransaction();
            try
            {
                while (ConcurrentInputChannel.TryReceive(out var value))
                {
                    events.Add(value);
                    Transaction(value.Value, value.UniqueId);
                    if ((DateTime.UtcNow - start).TotalMilliseconds > 100) break;//保证批量延时不超过100ms
                }
                await CommitTransaction();
                events.ForEach(evt => evt.TaskSource.SetResult(true));
            }
            catch
            {
                try
                {
                    await RollbackTransaction();
                    await EventsReTry(events);
                }
                catch (Exception e)
                {
                    foreach (var evt in events)
                    {
                        evt.TaskSource.TrySetException(e);
                    }
                }
            }
        }
        private async ValueTask EventsReTry(IList<EventTransactionWrap<K>> events)
        {
            foreach (var evt in events)
            {
                try
                {
                    evt.TaskSource.TrySetResult(await RaiseEvent(evt.Value, evt.UniqueId));
                }
                catch (Exception e)
                {
                    evt.TaskSource.TrySetException(e);
                }
            }
        }

    }
}
