using Ray.Core.Event;

namespace Ray.DistributedTx
{
    public class TxCommitEvent : IEvent
    {
        public long Id { get; set; }
        public long StartVersion { get; set; }
        public long StartTimestamp { get; set; }
        public TxCommitEvent() { }
        public TxCommitEvent(long transactionId, long startVersion, long startTimestamp)
        {
            Id = transactionId;
            StartVersion = startVersion;
            StartTimestamp = startTimestamp;
        }
    }
}
