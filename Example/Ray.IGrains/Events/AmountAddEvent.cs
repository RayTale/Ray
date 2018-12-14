using ProtoBuf;
using Ray.Core.Internal;
using System;

namespace Ray.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AmountAddEvent : IEventBase<long>
    {
        #region base
        public long Version { get; set; }
        public DateTime Timestamp { get; set; }
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
