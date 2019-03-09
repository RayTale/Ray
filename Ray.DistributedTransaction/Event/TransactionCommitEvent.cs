using Ray.Core.Event;

namespace Ray.DistributedTransaction
{
    public class TransactionCommitEvent : IEvent
    {
        public long Id { get; set; }
        public long StartVersion { get; set; }
        public long StartTimestamp { get; set; }
        public TransactionCommitEvent() { }
        public TransactionCommitEvent(long transactionId, long startVersion, long startTimestamp)
        {
            Id = transactionId;
            StartVersion = startVersion;
            StartTimestamp = startTimestamp;
        }
    }
}
