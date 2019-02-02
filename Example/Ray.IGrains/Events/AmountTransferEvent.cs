using ProtoBuf;
using Ray.Core.Event;

namespace Ray.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AmountTransferEvent : IEvent
    {
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
