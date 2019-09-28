using Ray.Core.Event;
using Ray.Core.Serialization;

namespace Transfer.Grains.Events
{
    [TCode(nameof(TransferRefundsEvent))]
    public class TransferRefundsEvent : IEvent
    {
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
