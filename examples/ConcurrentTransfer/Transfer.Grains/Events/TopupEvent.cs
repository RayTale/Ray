using Ray.Core.Event;
using Ray.Core.Serialization;

namespace Transfer.Grains.Events
{
    [EventName(nameof(TopupEvent))]
    public class TopupEvent : IEvent
    {
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
    }
}
