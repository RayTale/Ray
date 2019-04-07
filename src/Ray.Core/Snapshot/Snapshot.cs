namespace Ray.Core.Snapshot
{
    public class Snapshot<PrimaryKey, StateType>
        where StateType : class, new()
    {
        public Snapshot()
        {
        }
        public Snapshot(PrimaryKey stateId)
        {
            Base = new SnapshotBase<PrimaryKey>
            {
                StateId = stateId
            };
            State = new StateType();
        }
        public SnapshotBase<PrimaryKey> Base { get; set; }
        public StateType State { get; set; }
    }
}
