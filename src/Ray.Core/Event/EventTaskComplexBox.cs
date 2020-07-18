﻿using System;
using System.Threading.Tasks;

namespace Ray.Core.Event
{
    public class EventTaskComplexBox<Snapshot>
    {
        readonly TaskCompletionSource<bool> taskCompletionSource;
        public EventTaskComplexBox(
            string transactionId,
            Func<Snapshot, Func<IEvent, EventUID, Task>, Task> handler,
            TaskCompletionSource<bool> taskCompletionSource)
        {
            TransactionId = transactionId;
            Handler = handler;
            this.taskCompletionSource = taskCompletionSource;
        }
        public string TransactionId { get; set; }
        public bool Executed { get; set; }
        public Func<Snapshot, Func<IEvent, EventUID, Task>, Task> Handler { get; }
        public void Completed(bool result)
        {
            taskCompletionSource.TrySetResult(result);
        }
        public void Exception(Exception ex)
        {
            taskCompletionSource.TrySetException(ex);
        }
    }
}
