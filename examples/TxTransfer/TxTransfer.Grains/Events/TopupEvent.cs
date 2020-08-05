using Ray.Core.Event;

namespace TxTransfer.Grains.Events
{
    [EventName(nameof(TopupEvent))]
    public class TopupEvent : IEvent
    {
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
