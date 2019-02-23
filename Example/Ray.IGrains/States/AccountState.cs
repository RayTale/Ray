using ProtoBuf;
using Ray.Core.Snapshot;

namespace Ray.IGrains.States
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AccountState : ICloneable<AccountState>
    {
        public decimal Balance { get; set; }

        public AccountState Clone()
        {
            return new AccountState
            {
                Balance = Balance
            };
        }
    }
}
