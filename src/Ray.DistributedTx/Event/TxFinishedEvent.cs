using Ray.Core.Event;

namespace Ray.DistributedTransaction
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
