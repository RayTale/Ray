using Ray.Core.Snapshot;

namespace Transfer.Grains
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
