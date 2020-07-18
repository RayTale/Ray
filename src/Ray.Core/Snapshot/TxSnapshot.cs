namespace Ray.Core.Snapshot
{
    public class TxSnapshot<PrimaryKey, StateType> : Snapshot<PrimaryKey, StateType>
        where StateType : class, new()
    {
        public TxSnapshot()
        {
        }
        public TxSnapshot(PrimaryKey stateId)
        {
            Base = new TxSnapshotBase<PrimaryKey>
            {
                StateId = stateId
            };
            State = new StateType();
        }
    }
}
