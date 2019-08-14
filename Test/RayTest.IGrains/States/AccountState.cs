using Ray.Core.Snapshot;

namespace RayTest.IGrains.States
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
