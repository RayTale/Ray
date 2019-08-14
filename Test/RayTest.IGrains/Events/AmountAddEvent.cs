using Ray.Core.Event;

namespace RayTest.IGrains.Events
{
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
