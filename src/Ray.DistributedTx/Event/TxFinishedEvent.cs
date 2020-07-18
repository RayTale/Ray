using Ray.Core.Event;

namespace Ray.DistributedTx
{
    public class TxFinishedEvent : IEvent
    {
        public string Id { get; set; }
        public TxFinishedEvent() { }
        public TxFinishedEvent(string transactionId)
        {
            Id = transactionId;
        }
    }
}
