using Ray.Core.Event;

namespace Ray.DistributedTx
{
    public class TxCommitEvent : IEvent
    {
        public string Id { get; set; }

        public long StartVersion { get; set; }

        public long StartTimestamp { get; set; }

        public TxCommitEvent()
        {
        }

        public TxCommitEvent(string transactionId, long startVersion, long startTimestamp)
        {
            this.Id = transactionId;
            this.StartVersion = startVersion;
            this.StartTimestamp = startTimestamp;
        }
    }
}
