using Ray.Core.Event;

namespace Transfer.Grains.Events
{
    [EventName(nameof(TransferArrivedEvent))]
    public class TransferArrivedEvent : IEvent
    {
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
