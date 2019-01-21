using ProtoBuf;
using Ray.Core.State;

namespace RayTest.IGrains.States
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class AccountState : BaseState<long>, ICloneable<AccountState>
    {
        public override StateBase<long> Base { get; set; }
        public decimal Balance { get; set; }
        public AccountState Clone()
        {
            return new AccountState
            {
                Base = new StateBase<long>
                {
                    StateId = Base.StateId,
                    DoingVersion = Base.DoingVersion,
                    IsOver = Base.IsOver,
                    Version = Base.Version,
                    IsLatest = Base.IsLatest,
                    LatestMinEventTimestamp = Base.LatestMinEventTimestamp
                },
                Balance = Balance
            };
        }
    }
}
