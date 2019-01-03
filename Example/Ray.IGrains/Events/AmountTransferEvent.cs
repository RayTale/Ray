using System;
using ProtoBuf;
using Ray.Core.Abstractions;

namespace Ray.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AmountTransferEvent : IEventBase<long>
    {
        #region base
        public long Version { get; set; }
        public DateTime Timestamp { get; set; }
        public long StateId { get; set; }
        #endregion
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
