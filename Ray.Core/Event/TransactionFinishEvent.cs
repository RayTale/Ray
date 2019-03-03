using Ray.Core.Event;

namespace Ray.Core
{
    public class TransactionFinishEvent : IEvent
    {
        public long Id { get; set; }
        public TransactionFinishEvent() { }
        public TransactionFinishEvent(long transactionId)
        {
            Id = transactionId;
        }
    }
}
