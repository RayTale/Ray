using Ray.Core.Snapshot;

namespace TxTransfer.Grains.States
{
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
