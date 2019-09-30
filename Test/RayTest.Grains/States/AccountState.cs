using Ray.Core.Snapshot;

namespace RayTest.Grains
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
