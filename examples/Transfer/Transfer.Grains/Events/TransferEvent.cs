using Ray.Core.Event;
using Ray.Core.Serialization;

namespace Transfer.Grains.Events
{
    [TCode(nameof(TransferEvent))]
    public class TransferEvent : IEvent
    {
        public long ToId { get; set; }
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
