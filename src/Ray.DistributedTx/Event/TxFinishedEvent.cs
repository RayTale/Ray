using Ray.Core.Event;

namespace Ray.DistributedTx
{
    public class TxFinishedEvent : IEvent
    {
        public long Id { get; set; }
        public TxFinishedEvent() { }
        public TxFinishedEvent(long transactionId)
        {
            Id = transactionId;
        }
    }
}
