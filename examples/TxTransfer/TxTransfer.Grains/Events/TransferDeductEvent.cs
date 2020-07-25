using Ray.Core.Event;

namespace TxTransfer.Grains.Events
{
    [EventName(nameof(TransferDeductEvent))]
    public class TransferDeductEvent : IEvent
    {
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
