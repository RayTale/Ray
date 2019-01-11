using ProtoBuf;
using Ray.Core.Event;

namespace Ray.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AmountAddEvent : IActorEvent<long>
    {
        #region base
        public long Version { get; set; }
        public long Timestamp { get; set; }
        public long StateId { get; set; }
        #endregion
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
