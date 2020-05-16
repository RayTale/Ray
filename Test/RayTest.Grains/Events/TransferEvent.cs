using Ray.Core.Event;
using Ray.Core.Serialization;

namespace RayTest.Grains.Events
{
    [EventName(nameof(TransferEvent))]
    public class TransferEvent : IEvent
    {
        public long ToId { get; set; }
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
