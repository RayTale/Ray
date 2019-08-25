using System;
using System.Threading.Tasks;

namespace Ray.Core.Event
{
    public class ConcurrentTransport<Snapshot>
    {
        readonly TaskCompletionSource<bool> taskCompletionSource;
        public ConcurrentTransport(
            long transactionId,
            Func<Snapshot, Func<IEvent, EventUID, Task>, Task> handler,
            TaskCompletionSource<bool> taskCompletionSource)
        {
            TransactionId = transactionId;
            Handler = handler;
            this.taskCompletionSource = taskCompletionSource;
        }
        public long TransactionId { get; set; }
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
