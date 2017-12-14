using ProtoBuf;
using Ray.Core.EventSourcing;
using System;

namespace Ray.IGrains.States
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AccountState : IState<string>
    {
        #region base
        public string StateId { get; set; }
        public uint Version { get; set; }
        public DateTime VersionTime { get; set; }
        #endregion
        public decimal Balance { get; set; }
    }
}
