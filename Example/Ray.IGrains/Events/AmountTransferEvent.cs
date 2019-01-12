using ProtoBuf;

namespace Ray.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AmountTransferEvent : BaseEvent<long>
    {
        public override EventBase<long> Base { get; set; } = new EventBase<long>();
        public long ToAccountId { get; set; }
        public decimal Amount { get; set; }
        public decimal Balance { get; set; }
        public AmountTransferEvent() { }
        public AmountTransferEvent(long toAccountId, decimal amount, decimal balance)
        {
            ToAccountId = toAccountId;
            Amount = amount;
            Balance = balance;
        }
    }
}
