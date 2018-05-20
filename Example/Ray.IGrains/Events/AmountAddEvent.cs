using ProtoBuf;
using Ray.Core.EventSourcing;
using Ray.Core.Utils;
using System;

namespace Ray.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AmountAddEvent : IEventBase<string>
    {
        #region base
        public Int64 Version { get; set; }
        public DateTime Timestamp { get; set; }
        public string StateId { get; set; }

        public string TypeCode => GetType().FullName;
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
