using Ray.Core.Event;

namespace Transfer.Grains.Events
{
    [EventName(nameof(TransferDeductEvent))]
    public class TransferDeductEvent : IEvent
    {
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
