using ProtoBuf;
using Ray.Core.State;

namespace RayTest.IGrains.States
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AccountState : IState<long>, ICloneable<AccountState>
    {
        #region base
        public long StateId { get; set; }
        public long Version { get; set; }
        public long DoingVersion { get; set; }
        #endregion
        public decimal Balance { get; set; }

        public AccountState Clone()
        {
            return new AccountState
            {
                StateId = StateId,
                Version = Version,
                DoingVersion = DoingVersion,
                Balance = Balance
            };
        }
    }
}
