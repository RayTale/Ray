using Ray.Core.Event;

namespace Transfer.Grains.Events
{
    [EventName(nameof(TransferEvent))]
    public class TransferEvent : IEvent
    {
        public long ToId { get; set; }
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
