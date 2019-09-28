using Ray.Core.Event;
using Ray.Core.Serialization;

namespace Ray.Grain.Events
{
    [TCode(nameof(AmountAddEvent))]
    public class AmountAddEvent : IEvent
    {
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
        public AmountAddEvent() { }
        public AmountAddEvent(decimal amount, decimal balance)
        {
            Amount = amount;
            Balance = balance;
        }
    }
}
