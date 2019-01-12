using ProtoBuf;

namespace RayTest.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AmountAddEvent : BaseEvent<long>
    {
        public override EventBase<long> Base { get; set; } = new EventBase<long>();
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
