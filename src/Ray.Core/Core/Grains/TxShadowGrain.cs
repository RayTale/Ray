using Microsoft.Extensions.Logging;
using Ray.Core.Snapshot;
using System.Threading.Tasks;

namespace Ray.Core.Core.Grains
{
    public abstract class TxShadowGrain<Main, PrimaryKey, StateType> : ShadowGrain<Main, PrimaryKey, StateType>
        where StateType : class, new()
    {
        protected override ValueTask CreateSnapshot()
        {
            Snapshot = new TxSnapshot<PrimaryKey, StateType>(GrainId);
            return Consts.ValueTaskDone;
        }
        protected override async Task ReadSnapshotAsync()
        {
            await base.ReadSnapshotAsync();
            Snapshot = new TxSnapshot<PrimaryKey, StateType>()
            {
                Base = new TxSnapshotBase<PrimaryKey>(Snapshot.Base),
                State = Snapshot.State
            };
        }
    }
}
