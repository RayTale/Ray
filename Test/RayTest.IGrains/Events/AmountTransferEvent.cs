using ProtoBuf;
using Ray.Core.Internal;
using Ray.Core.Utils;
using System;

namespace RayTest.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AmountTransferEvent : IEventBase<long>
    {
        #region base
        public Int64 Version { get; set; }
        public DateTime Timestamp { get; set; }
        public long StateId { get; set; }
        public string TypeCode => GetType().FullName;
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
